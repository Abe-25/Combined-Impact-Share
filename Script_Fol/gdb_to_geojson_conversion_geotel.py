from __future__ import annotations
import argparse
import os
import csv
import json
import logging
import re
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import requests
import yaml
from requests.adapters import HTTPAdapter
from azure.core.pipeline.transport import RequestsTransport
from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.ay_akv import AyAzureKeyVault

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


class PatchedRequestsTransport(RequestsTransport):
    def __init__(self, *, pool: int = 200):
        s = requests.Session()
        a = HTTPAdapter(pool_connections=pool, pool_maxsize=pool)
        s.mount("https://", a)
        s.mount("http://", a)
        super().__init__(session=s, session_owner=True)

    def send(self, request, *, proxies=None, **kwargs):
        kwargs.pop("hosts", None)
        kwargs.pop("timeout", None)
        return super().send(request, proxies=proxies, **kwargs)


def norm_blob_path(p: str) -> str:
    p = (p or "").strip().lstrip("/").replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    return p


def norm_prefix(p: str) -> str:
    p = norm_blob_path(p)
    return p + "/" if p and not p.endswith("/") else p


def required_key(cfg: dict, dotted: str):
    cur: Any = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(f"Missing required config key: '{dotted}'")
        cur = cur[part]
    if cur is None or (isinstance(cur, str) and not cur.strip()):
        raise ValueError(f"Config key '{dotted}' is empty")
    return cur


def load_env_cfg(env: str, config_path: Optional[str] = None) -> Dict[str, Any]:
    cfg_file = (
        Path(config_path).expanduser().resolve()
        if config_path
        else Path(__file__).with_name("config.yml")
    )
    if not cfg_file.exists():
        raise FileNotFoundError(f"config.yml not found at: {cfg_file}")

    all_cfg = yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
    if env not in all_cfg:
        raise KeyError(
            f"Env '{env}' not found in {cfg_file}. Available: {list(all_cfg.keys())}"
        )

    cfg = all_cfg[env] or {}
    required_key(cfg, "key_vault")
    required_key(cfg, "blob_storage.secret_name")
    required_key(cfg, "blob_storage.container_name")
    if not (
        cfg.get("blob_storage", {}).get("folder_prefix")
        or cfg.get("blob_storage", {}).get("base_prefix")
    ):
        raise KeyError(
            "Missing required config key: 'blob_storage.folder_prefix' or 'blob_storage.base_prefix'"
        )
    return cfg


def has_cmd(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def run_cmd(cmd: List[str]) -> Tuple[int, str, str]:
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, p.stdout or "", p.stderr or ""


def gdal_has_openfilegdb() -> bool:
    if not has_cmd("ogrinfo"):
        return False
    rc, out, _ = run_cmd(["ogrinfo", "--formats"])
    return rc == 0 and "OpenFileGDB" in out


def diag() -> None:
    logging.info(
        "Diagnostics: ogrinfo=%s ogr2ogr=%s OpenFileGDB=%s",
        has_cmd("ogrinfo"),
        has_cmd("ogr2ogr"),
        gdal_has_openfilegdb(),
    )
    if has_cmd("ogrinfo"):
        rc, out, _ = run_cmd(["ogrinfo", "--version"])
        if rc == 0:
            logging.info("ogrinfo version: %s", out.strip())


def pick_temp_root() -> Path:
    def _candidates() -> list[Path]:
        env = (
            os.environ.get("GDB2GEOJSON_WORKDIR") or os.environ.get("WORK_DIR") or ""
        ).strip()
        c: list[Path] = []
        if env:
            c.append(Path(env))
        c.extend(
            [
                Path("/mnt/work"),
                Path("/mnt"),
                Path("/data"),
                Path("/var/tmp"),
                Path(tempfile.gettempdir()),
            ]
        )
        seen: set[str] = set()
        out: list[Path] = []
        for p in c:
            ps = str(p)
            if ps not in seen:
                seen.add(ps)
                out.append(p)
        return out

    min_free_gb = float(os.environ.get("GDB2GEOJSON_MIN_FREE_GB", "1"))
    min_free_bytes = int(min_free_gb * (1024**3))
    best: tuple[int, Path] | None = None

    for p in _candidates():
        try:
            p.mkdir(parents=True, exist_ok=True)
            test = p / ".__writetest__"
            test.write_text("ok", encoding="utf-8")
            test.unlink(missing_ok=True)
            free = shutil.disk_usage(p).free
            if free < min_free_bytes:
                logging.info(
                    "Temp candidate %s skipped: free=%0.2fGB < min=%0.2fGB",
                    p,
                    free / (1024**3),
                    min_free_gb,
                )
                continue
            if best is None or free > best[0]:
                best = (free, p)
        except Exception as e:
            logging.info("Temp candidate %s skipped: %s", p, e)
            continue

    if best is None:
        p = Path(tempfile.gettempdir())
        p.mkdir(parents=True, exist_ok=True)
        best = (shutil.disk_usage(p).free, p)

    free, root = best
    logging.info("Using temp root: %s (free=%0.2fGB)", root, free / (1024**3))
    return root


def download_blob(
    container: ContainerClient,
    blob_name: str,
    local_path: Path,
    *,
    max_concurrency: int = 4,
    timeout: int = 1200,
    retries: int = 6,
) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    bc = container.get_blob_client(blob_name)
    last: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            with local_path.open("wb") as f:
                stream = bc.download_blob(
                    max_concurrency=max_concurrency, timeout=timeout
                )
                stream.readinto(f)
            return
        except Exception as e:
            last = e
            wait = min(2**attempt, 30)
            logging.warning(
                "Download failed (%s/%s) blob=%s err=%s; retry in %ss",
                attempt,
                retries,
                blob_name,
                e,
                wait,
            )
            time.sleep(wait)

    raise last


def download_gdb(
    container: ContainerClient,
    gdb_root: str,
    target_dir: Path,
    *,
    workers: int = 4,
    per_blob_concurrency: int = 1,
) -> Path:
    gdb_root = norm_blob_path(gdb_root).rstrip("/")
    prefix = norm_prefix(gdb_root)
    blobs = [b.name for b in container.list_blobs(name_starts_with=prefix)]
    if not blobs:
        raise RuntimeError(f"No blobs found under {prefix}")

    target_dir.mkdir(parents=True, exist_ok=True)
    logging.info("Downloading GDB: %s (%d blobs)", gdb_root, len(blobs))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [
            ex.submit(
                download_blob,
                container,
                name,
                target_dir / norm_blob_path(name)[len(prefix) :],
                max_concurrency=per_blob_concurrency,
            )
            for name in blobs
        ]
        for f in as_completed(futs):
            f.result()

    return target_dir


def upload_file(
    container: ContainerClient, local_file: Path, blob_path: str, overwrite: bool = True
) -> None:
    blob_path = norm_blob_path(blob_path)
    ct = (
        "application/geo+json"
        if local_file.suffix.lower() in {".geojson", ".json"}
        else "application/octet-stream"
    )
    with local_file.open("rb") as f:
        container.get_blob_client(blob_path).upload_blob(
            f,
            overwrite=overwrite,
            content_settings=ContentSettings(content_type=ct),
        )


def list_gdb_roots(container: ContainerClient, base_prefix: str) -> List[str]:
    pfx = norm_prefix(base_prefix)
    roots: Set[str] = set()
    for b in container.list_blobs(name_starts_with=pfx):
        n = norm_blob_path(b.name)
        lo = n.lower()
        if ".gdb/" in lo:
            roots.add(n[: lo.index(".gdb/") + len(".gdb")])
    return sorted(roots)


def safe_filename(s: str) -> str:
    s = re.sub(r"[^\w\-.]+", "_", (s or "").strip())
    return s[:180] if len(s) > 180 else s


def ogrsql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def list_layers(dataset: str) -> List[str]:
    if not has_cmd("ogrinfo"):
        return []
    rc, out, err = run_cmd(["ogrinfo", "-ro", "-q", dataset])
    if rc != 0:
        logging.debug("ogrinfo failed: %s", err.strip())
        return []

    layers: List[str] = []
    for ln in out.splitlines():
        # m = re.match(r"^\s*\d+\s*:\s*(.+?)\s*\(", ln)
        m = re.match(r"^.*:\s*(.+?)\s*\(", ln)
        if m:
            layers.append(m.group(1).strip())
    return layers


def layer_has_geom(dataset: str, layer: Optional[str] = None) -> bool:
    if not has_cmd("ogrinfo"):
        return False
    cmd = ["ogrinfo", "-ro", "-so", dataset] + ([layer] if layer else [])
    rc, out, _ = run_cmd(cmd)
    if rc != 0:
        return False
    for ln in out.splitlines():
        if ln.strip().lower().startswith("geometry:"):
            return "none" not in ln.lower()
    return False


def ogr2geojson(
    dataset: str, out: Path, epsg: int, layer: Optional[str] = None
) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)

    cmd = ["ogr2ogr", "-f", "GeoJSON", str(out), dataset]

    if layer:
        cmd += [
            "-dialect",
            "OGRSQL",
            "-sql",
            f"SELECT fid AS OBJECTID, * FROM {ogrsql_ident(layer)}",
        ]

    cmd += ["-t_srs", f"EPSG:{epsg}", "-lco", "RFC7946=YES"]
    rc, _, err = run_cmd(cmd)
    if rc != 0:
        raise RuntimeError(err.strip() or "ogr2ogr failed")


def ogr2csv(dataset: str, out: Path, layer: Optional[str] = None) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)

    cmd = ["ogr2ogr", "-f", "CSV", str(out), dataset]

    if layer:
        cmd += [
            "-dialect",
            "OGRSQL",
            "-sql",
            f"SELECT fid AS OBJECTID, * FROM {ogrsql_ident(layer)}",
        ]

    rc, _, err = run_cmd(cmd)
    if rc != 0:
        raise RuntimeError(err.strip() or "ogr2ogr CSV failed")


def csv_to_geojson_null(csv_path: Path, out: Path, name: Optional[str] = None) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        feats = [
            {"type": "Feature", "geometry": None, "properties": (row or {})}
            for row in rdr
        ]
    out.write_text(
        json.dumps(
            {"type": "FeatureCollection", "name": name or out.stem, "features": feats},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def gdb_diag(p: Path) -> str:
    if not p.exists():
        return "does not exist"
    if not p.is_dir():
        return "is not a directory"
    return f"files={len(list(p.glob('*')))}"


def convert_gdb_layers(
    gdb_dir: Path,
    output_dir: Path,
    target_epsg: int = 4326,
    include_layers: Optional[Iterable[str]] = None,
    exclude_layers: Optional[Iterable[str]] = None,
    include_nonspatial_tables: bool = True,
) -> Iterator[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    inc, exc = set(include_layers or []), set(exclude_layers or [])

    if not (has_cmd("ogrinfo") and has_cmd("ogr2ogr")):
        raise RuntimeError(
            "Missing GDAL CLI tools (ogrinfo/ogr2ogr). Install gdal-bin."
        )
    if not gdal_has_openfilegdb():
        raise RuntimeError(
            "GDAL OpenFileGDB driver not found. Install GDAL with OpenFileGDB support."
        )

    ds, done = str(gdb_dir), False
    layers = list_layers(ds)

    if layers:
        for lyr in layers:
            if inc and lyr not in inc:
                continue
            if exc and lyr in exc:
                continue

            out = output_dir / f"{safe_filename(lyr)}.geojson"
            if layer_has_geom(ds, lyr):
                logging.info("Converting spatial layer: %s", lyr)
                ogr2geojson(ds, out, target_epsg, lyr)
                done = True
                yield out
            elif include_nonspatial_tables:
                logging.info("Converting non-spatial table: %s", lyr)
                tmp = output_dir / f"{safe_filename(lyr)}.csv"
                try:
                    ogr2csv(ds, tmp, lyr)
                    csv_to_geojson_null(tmp, out, lyr)
                    done = True
                    yield out
                finally:
                    tmp.unlink(missing_ok=True)

        if done:
            return

    logging.info("No layers listed for %s, scanning *.gdbtable", ds)
    tables = sorted(gdb_dir.glob("*.gdbtable"))
    if not tables:
        raise RuntimeError(f"No *.gdbtable found for {gdb_dir} ({gdb_diag(gdb_dir)})")

    for t in tables:
        name = t.stem
        if inc and name not in inc:
            continue
        if exc and name in exc:
            continue

        out = output_dir / f"{safe_filename(name)}.geojson"
        try:
            logging.info("Converting gdbtable (GeoJSON attempt): %s", t.name)
            ogr2geojson(str(t), out, target_epsg)
            done = True
            yield out
            continue
        except Exception as e:
            logging.debug("GeoJSON failed for %s: %s", t.name, e)

        if not include_nonspatial_tables:
            continue

        tmp = output_dir / f"{safe_filename(name)}.csv"
        try:
            logging.info("Converting gdbtable (CSV->GeoJSON null): %s", t.name)
            ogr2csv(str(t), tmp)
            csv_to_geojson_null(tmp, out, name)
            done = True
            yield out
        except Exception as e:
            logging.info("Skipping table %s: %s", t.name, e)
        finally:
            tmp.unlink(missing_ok=True)

    if not done:
        raise RuntimeError(
            f"No layers/tables converted from {gdb_dir} ({gdb_diag(gdb_dir)})"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev", type=str)
    ap.add_argument("--config", default=None, type=str)
    ap.add_argument("--run_date", required=False, type=str)
    ap.add_argument("--diag", action="store_true")
    args = ap.parse_args()

    if args.diag:
        diag()

    env_arg = (args.env or "").lower().strip()
    envs = (
        ["dev", "prd"]
        if not any(a == "--env" or a.startswith("--env=") for a in sys.argv[1:])
        else [env_arg]
    )
    if any(e not in {"dev", "prd"} for e in envs):
        raise ValueError(
            f"{env_arg} is not a valid environment. Valid options are only dev and prd."
        )

    azure_kv = AyAzureKeyVault()

    for env in envs:
        cfg = load_env_cfg(env, args.config)
        logging.info("Processing environment: %s", env)

        vault_uri = f"https://{required_key(cfg, 'key_vault')}.vault.azure.net/"
        secret_name = required_key(cfg, "blob_storage.secret_name")
        container_name = required_key(cfg, "blob_storage.container_name")
        base_prefix = norm_blob_path(
            str(
                cfg.get("blob_storage", {}).get("folder_prefix")
                or cfg.get("blob_storage", {}).get("base_prefix")
            )
        )
        processed_subdir = (
            str(
                cfg.get("blob_storage", {}).get("processed_subdir")
                or cfg.get("blob_storage", {}).get("output_subdir")
                or "processed"
            )
            .strip()
            .strip("/")
        )
        overwrite_geojson = bool(
            cfg.get("blob_storage", {}).get("overwrite_geojson", True)
        )
        continue_on_error = bool(
            cfg.get("blob_storage", {}).get("continue_on_error", True)
        )

        ing = cfg.get("ingestion", {}) or {}
        target_epsg = int(ing.get("target_epsg", 4326))
        include_layers = ing.get("include_layers") or []
        exclude_layers = ing.get("exclude_layers") or []
        include_nonspatial_tables = bool(ing.get("include_nonspatial_tables", True))

        conn = azure_kv.get_secret(vault_uri, secret_name)
        if not conn:
            raise RuntimeError("Blob storage connection string is empty.")

        transport = PatchedRequestsTransport(pool=200)
        blob_service = BlobServiceClient.from_connection_string(
            conn, transport=transport, retry_total=12, retry_backoff_factor=0.8
        )
        container = blob_service.get_container_client(container_name)
        logging.info("Azure client configured pool=%s retries=%s", 200, 12)

        roots = list_gdb_roots(container, base_prefix)
        if not roots:
            raise RuntimeError(f"No .gdb found under prefix: {base_prefix}")

        logging.info(
            "Found %d GDB root(s) under %s", len(roots), norm_prefix(base_prefix)
        )

        temp_root = pick_temp_root()
        with tempfile.TemporaryDirectory(dir=str(temp_root)) as td:
            td = Path(td)

            for r in roots:
                posix = PurePosixPath(r)
                parent = str(posix.parent) if str(posix.parent) != "." else ""
                out_prefix = norm_prefix(
                    "/".join([p for p in [parent, processed_subdir, posix.stem] if p])
                )
                local_gdb_path = td / "gdb" / posix.name
                local_out_path = td / "out" / posix.stem

                try:
                    local_gdb = download_gdb(
                        container, r, local_gdb_path, workers=4, per_blob_concurrency=1
                    )
                    local_out = local_out_path
                    outs = list(
                        convert_gdb_layers(
                            local_gdb,
                            local_out,
                            target_epsg,
                            include_layers,
                            exclude_layers,
                            include_nonspatial_tables,
                        )
                    )
                    logging.info("Converted %d output(s) for %s", len(outs), r)

                    for f in outs:
                        dst = out_prefix + f.name
                        upload_file(container, f, dst, overwrite=overwrite_geojson)
                        logging.info("Uploaded: %s", dst)

                except Exception as e:
                    logging.info("Failed for %s: %s", r, e)
                    if not continue_on_error:
                        raise
                finally:
                    shutil.rmtree(local_out_path, ignore_errors=True)
                    shutil.rmtree(local_gdb_path, ignore_errors=True)


if __name__ == "__main__":
    main()
