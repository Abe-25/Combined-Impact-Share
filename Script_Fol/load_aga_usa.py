from __future__ import annotations
import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import snowflake.connector
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

sys.path.append(str(Path(__file__).parent.parent.absolute()))
from utils.ay_akv import AyAzureKeyVault

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)


def norm_blob_path(p: str) -> str:
    p = (p or "").strip().lstrip("/").replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    return p


def norm_prefix(prefix: str) -> str:
    p = norm_blob_path(prefix)
    if p and not p.endswith("/"):
        p += "/"
    return p


def required_key(cfg: dict, dotted_key: str):
    cur: Any = cfg
    for part in dotted_key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(f"Missing required config key: '{dotted_key}'")
        cur = cur[part]
    if cur is None or (isinstance(cur, str) and not cur.strip()):
        raise ValueError(f"Config key '{dotted_key}' is empty")
    return cur


def storage_account_from_conn(conn_str: str) -> str:
    m = re.search(r"AccountName=([^;]+)", conn_str)
    if not m:
        raise ValueError("Could not parse AccountName from Azure connection string")
    return m.group(1)


def pem_secret_to_der_private_key(pem_text: str) -> bytes:
    key = serialization.load_pem_private_key(
        pem_text.encode("utf-8"),
        password=None,
        backend=default_backend(),
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def qident(name: str) -> str:
    name = (name or "").replace('"', '""')
    return f'"{name}"'


def qname(*parts: str) -> str:
    return ".".join(qident(p) for p in parts if p is not None and str(p).strip() != "")


def exec_many(cur, statements: Iterable[str]) -> None:
    for stmt in statements:
        s = (stmt or "").strip()
        if s:
            cur.execute(s)


def ensure_columns(
    cur, database: str, schema: str, table: str, desired: List[Tuple[str, str]]
) -> None:
    sql = f"""
        SELECT UPPER(column_name)
          FROM {qident(database)}.INFORMATION_SCHEMA.COLUMNS
         WHERE UPPER(table_schema) = UPPER(%s)
           AND UPPER(table_name) = UPPER(%s)
    """
    cur.execute(sql, (schema, table))
    existing = {row[0] for row in (cur.fetchall() or [])}

    alters: List[str] = []
    for col, ddl_type in desired:
        if col.upper() not in existing:
            alters.append(
                f"ALTER TABLE {qname(database, schema, table)} ADD COLUMN {qident(col)} {ddl_type}"
            )

    if alters:
        logger.info(
            "Altering %s.%s.%s to add %s column(s)",
            database,
            schema,
            table,
            len(alters),
        )
        exec_many(cur, alters)


def sf_ident(s: str, fallback: str = "X") -> str:
    s = re.sub(r"[^0-9A-Za-z_]+", "_", (s or "").strip())
    s = re.sub(r"__+", "_", s).strip("_") or fallback
    return ("_" + s if re.match(r"^\d", s) else s).upper()


def dataset_table_name(dataset: str, prefixes: List[str]) -> str:
    d = (dataset or "").strip()
    for p in (prefixes or []):
        if d.upper().startswith((p or "").upper()):
            d = d[len(p) :]
            break
    return sf_ident(d, "DATASET")


def dataset_year2(dataset: str) -> str:
    d = dataset or ""
    m = re.search(r"(19|20)\d{2}(?!.*(19|20)\d{2})", d)
    if m:
        return m.group(0)[-2:]
    m = re.search(r"(\d{2})(?!.*\d{2})", d)
    return m.group(1) if m else ""


def auto_col_name(prop_key: str, dataset: str) -> str:
    k = (prop_key or "").strip()
    if k.upper() == "BL":
        return "BL_ID"
    m = re.match(r"^([A-Za-z]+)(\d{2})$", k)
    y2 = dataset_year2(dataset)
    return f"{sf_ident(m.group(1))}_ID" if (m and y2 and m.group(2) == y2) else sf_ident(k)


PROP_TO_COL_RULES: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"^csdcy(?:_.*)?$", re.IGNORECASE), "CSACY"),
    (re.compile(r"^placecy$", re.IGNORECASE), "PL_ID"),
]


def canonical_prop_to_col(prop_key: str, dataset: str, rename_map: Dict[str, str] = None) -> str:
    """
    Map a GeoJSON property key to a Snowflake column name.
    """
    if rename_map and prop_key in rename_map and str(rename_map[prop_key]).strip():
        return sf_ident(rename_map[prop_key])

    k = (prop_key or "").strip()
    for rx, target in PROP_TO_COL_RULES:
        if rx.match(k):
            return sf_ident(target)

    return sf_ident(auto_col_name(prop_key, dataset))


def preferred_column_order(col_names: List[str]) -> List[str]:
    existing = {c.upper() for c in (col_names or [])}
    if "ZIP" in existing:
        pref = ["ZIP", "NAME", "STABB", "AREA", "TYPE", "ENZIP"]
        return [c for c in pref if c in existing]
    if "TR_ID" in existing and "CT_ID" in existing:
        pref = ["BG_ID", "TR_ID", "CT_ID"]
        return [c for c in pref if c in existing]
    return []


def table_exists(cur, database: str, schema: str, table: str) -> bool:
    cur.execute(
        f"SELECT 1 FROM {qident(database)}.INFORMATION_SCHEMA.TABLES "
        "WHERE UPPER(table_schema)=UPPER(%s) AND UPPER(table_name)=UPPER(%s) LIMIT 1",
        (schema, table),
    )
    return cur.fetchone() is not None


def property_keys(cur, database: str, schema: str, base_table: str, dataset: str) -> List[str]:
    cur.execute(
        f"""
        SELECT DISTINCT f.value::string
          FROM {qname(database, schema, base_table)} t,
               LATERAL FLATTEN(input => OBJECT_KEYS(t.properties)) f
         WHERE t.dataset = %s AND t.properties IS NOT NULL
        """,
        (dataset,),
    )
    return [r[0] for r in (cur.fetchall() or []) if r and r[0]]


def build_dataset_tables(cur, cfg: Dict[str, Any], database: str, schema: str, base_table: str) -> None:
    """
    Create per-dataset tables
    """
    dt = (cfg or {}).get("dataset_tables") or {}
    if not dt.get("enabled", False):
        logger.info("dataset_tables.enabled=false; skipping per-dataset table build")
        return

    prefixes = dt.get("strip_prefixes") or ["USA_"]
    tmpl = dt.get("table_name_template") or "{dataset_table}"
    geom_src = (dt.get("geometry_source") or "geom_geography").strip().lower()
    geom_expr = "t.geom_geometry" if geom_src == "geom_geometry" else "t.geom_geography"
    auto_discover = bool(dt.get("auto_discover_properties", True))
    recreate = bool(dt.get("recreate_existing", False))
    id_first = bool(dt.get("id_first", True))
    mappings = dt.get("property_mappings") or {}

    cur.execute(f"SELECT DISTINCT dataset FROM {qname(database, schema, base_table)} WHERE dataset IS NOT NULL")
    datasets = [r[0] for r in (cur.fetchall() or []) if r and r[0]]
    logger.info("Found %s distinct dataset(s) in %s", len(datasets), base_table)

    for ds in datasets:
        dataset_table = dataset_table_name(ds, prefixes)
        table_name = tmpl.format(dataset_table=dataset_table, dataset=sf_ident(ds), base=base_table)

        if table_exists(cur, database, schema, table_name) and not recreate:
            logger.info("Dataset table exists, skipping: %s (dataset=%s)", table_name, ds)
            continue

        rename_map = mappings.get(ds) or mappings.get(sf_ident(ds)) or {}
        keys = property_keys(cur, database, schema, base_table, ds) if auto_discover else list(rename_map.keys())

        seen, cols = set(), []
        for k in keys:
            col = canonical_prop_to_col(k, ds, rename_map)
            base = col
            i = 2
            while col in seen:
                col = f"{base}_{i}"
                i += 1
            seen.add(col)
            cols.append((col, k))

        pref = preferred_column_order([c for c, _ in cols])
        pref_index = {c: i for i, c in enumerate(pref)}

        def sort_key(item):
            c = item[0]
            cu = c.upper()
            if cu in pref_index:
                return (0, pref_index[cu], cu)
            if id_first and cu.endswith("_ID"):
                return (1, 0, cu)
            return (2, 0, cu)

        cols.sort(key=sort_key)

        select_list = [f"t.properties:{k}::string AS {qident(c)}" for c, k in cols]

        need_snow_geo = bool(re.search(r"_bl_2", (ds or ""), flags=re.IGNORECASE))

        select_list += [
            f"{geom_expr} AS {qident('GEOMETRY')}",
        ]
        if need_snow_geo:
            select_list.append(f"ST_ASWKT({geom_expr})::string AS {qident('SNOW_GEO')}")

        select_list += [
            f"t.dataset AS {qident('FILE_NAME')}",
            f"t.filename AS {qident('FILE_PATH')}",
        ]

        ddl = "CREATE OR REPLACE TABLE" if recreate else "CREATE TABLE"
        sql = f"""
        {ddl} {qname(database, schema, table_name)} AS
        SELECT
            {',\n    '.join(select_list)}
        FROM {qname(database, schema, base_table)} t
        WHERE t.dataset = %s
        """
        logger.info("Creating dataset table %s for dataset=%s", table_name, ds)
        cur.execute(sql, (ds,))


def copy_result_rows_loaded(cur) -> int:
    total = 0
    rows = cur.fetchall() or []
    for r in rows:
        if len(r) >= 4 and isinstance(r[3], (int, float)):
            total += int(r[3])
    return total


def build_pattern(base_prefix: str, processed_subdir: str) -> str:
    base = norm_prefix(base_prefix).rstrip("/")
    sub = (processed_subdir or "processed").strip().strip("/")
    return f"^{re.escape(base)}.*/{re.escape(sub)}/.*\\.geojson$"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev", type=str, help="Project Environment (dev/prd)")
    parser.add_argument("--run_date", required=False, type=str, help="Run Date (optional)")
    parser.add_argument("--force_copy", action="store_true", help="Force Snowflake COPY")
    args = parser.parse_args()

    env_arg = (args.env or "").lower().strip()
    env_provided = any(a == "--env" or a.startswith("--env=") for a in sys.argv[1:])
    envs = ["dev", "prd"] if not env_provided else [env_arg]

    if any(e not in {"dev", "prd"} for e in envs):
        raise ValueError(f"{env_arg} is not a valid environment. Valid options are only dev and prd.")

    cfg_path = Path(__file__).with_name("config.yml")
    with cfg_path.open("r", encoding="utf-8") as stream:
        all_cfg = yaml.safe_load(stream)

    azure_kv = AyAzureKeyVault()

    for env in envs:
        cfg: Dict[str, Any] = all_cfg[env]
        key_vault_name = required_key(cfg, "key_vault")
        vault_uri = f"https://{key_vault_name}.vault.azure.net/"

        az_blob_secret_name = required_key(cfg, "blob_storage.secret_name")
        container_name = required_key(cfg, "blob_storage.container_name")
        base_prefix = cfg.get("blob_storage", {}).get("folder_prefix") or required_key(cfg, "blob_storage.base_prefix")
        processed_subdir = (
            cfg.get("blob_storage", {}).get("processed_subdir")
            or cfg.get("blob_storage", {}).get("output_subdir")
            or "processed"
        )

        connection_string = azure_kv.get_secret(vault_uri, az_blob_secret_name)
        storage_account = storage_account_from_conn(connection_string)
        stage_url = f"azure://{storage_account}.blob.core.windows.net/{container_name}"
        pattern = build_pattern(str(base_prefix), str(processed_subdir))

        logger.info("Stage URL: %s", stage_url)
        logger.info("COPY PATTERN: %s", pattern)

        for sf_cfg in required_key(cfg, "snowflake"):
            account = required_key(sf_cfg, "account")
            database = required_key(sf_cfg, "database")
            schema = required_key(sf_cfg, "schema")
            stage = required_key(sf_cfg, "stage")
            user = required_key(sf_cfg, "user")
            role = required_key(sf_cfg, "role")
            warehouse = required_key(sf_cfg, "warehouse")
            private_key_secret_name = required_key(sf_cfg, "private_key.secret_name")

            storage_integration = sf_cfg.get("storage_integration", "ETL_PRODUCT_INTEGRATION_LND")
            target_table = sf_cfg.get("target_table", "GEOJSON_GEOM")
            raw_lines_table = sf_cfg.get("raw_table") or f"{target_table}_RAW"
            load_mode = (sf_cfg.get("load_mode", "append") or "append").lower()
            dataset_level = int(sf_cfg.get("dataset_level", 3))

            force_copy = (
                bool(args.force_copy)
                or bool(sf_cfg.get("force_copy", False))
                or load_mode in {"truncate", "replace"}
            )

            pem_key = azure_kv.get_secret(vault_uri, private_key_secret_name)
            private_key_der = pem_secret_to_der_private_key(pem_key)

            logger.info("Connecting to Snowflake db=%s schema=%s dataset=%s", database, schema, sf_cfg.get("name", ""))

            conn = snowflake.connector.connect(
                account=account,
                user=user,
                role=role,
                warehouse=warehouse,
                database=database,
                schema=schema,
                private_key=private_key_der,
            )
            conn.autocommit(True)
            cur = conn.cursor()

            try:
                exec_many(
                    cur,
                    [
                        f"""
                        CREATE OR REPLACE FILE FORMAT {qname(database, schema, 'GEOJSON_LINE_FF')}
                          TYPE = CSV
                          FIELD_DELIMITER = '|'
                          RECORD_DELIMITER = '\\n'
                          SKIP_HEADER = 0
                          TRIM_SPACE = FALSE
                          FIELD_OPTIONALLY_ENCLOSED_BY = NONE
                          ESCAPE_UNENCLOSED_FIELD = NONE
                          NULL_IF = ('')
                        """,
                        f"""
                        CREATE OR REPLACE STAGE {qname(database, schema, stage)}
                          URL = '{stage_url}'
                          STORAGE_INTEGRATION = {qident(storage_integration)}
                          FILE_FORMAT = {qname(database, schema, 'GEOJSON_LINE_FF')}
                        """,
                    ],
                )

                create_raw = f"""
                    CREATE TABLE IF NOT EXISTS {qname(database, schema, raw_lines_table)} (
                      filename STRING,
                      file_row_number NUMBER,
                      line STRING,
                      ingested_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                      processed BOOLEAN DEFAULT FALSE
                    )
                """

                create_final = f"""
                    CREATE TABLE IF NOT EXISTS {qname(database, schema, target_table)} (
                      dataset STRING,
                      filename STRING,
                      file_row_number NUMBER,
                      feature_index NUMBER,
                      geometry_type STRING,
                      feature VARIANT,
                      properties VARIANT,
                      geom VARIANT,
                      geom_geography GEOGRAPHY,
                      geom_geometry GEOMETRY,
                      load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                """

                if load_mode == "replace":
                    create_raw = create_raw.replace("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE")
                    create_final = create_final.replace("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE")

                exec_many(cur, [create_raw, create_final])

                ensure_columns(
                    cur,
                    database,
                    schema,
                    raw_lines_table,
                    [
                        ("FILENAME", "STRING"),
                        ("FILE_ROW_NUMBER", "NUMBER"),
                        ("LINE", "STRING"),
                        ("INGESTED_TS", "TIMESTAMP_LTZ"),
                        ("PROCESSED", "BOOLEAN"),
                    ],
                )

                ensure_columns(
                    cur,
                    database,
                    schema,
                    target_table,
                    [
                        ("DATASET", "STRING"),
                        ("FILENAME", "STRING"),
                        ("FILE_ROW_NUMBER", "NUMBER"),
                        ("FEATURE_INDEX", "NUMBER"),
                        ("GEOMETRY_TYPE", "STRING"),
                        ("FEATURE", "VARIANT"),
                        ("PROPERTIES", "VARIANT"),
                        ("GEOM", "VARIANT"),
                        ("GEOM_GEOGRAPHY", "GEOGRAPHY"),
                        ("GEOM_GEOMETRY", "GEOMETRY"),
                        ("LOAD_TS", "TIMESTAMP_LTZ"),
                    ],
                )

                if load_mode in {"truncate", "replace"}:
                    exec_many(
                        cur,
                        [
                            f"TRUNCATE TABLE {qname(database, schema, raw_lines_table)}",
                            f"TRUNCATE TABLE {qname(database, schema, target_table)}",
                        ],
                    )

                cur.execute(f"LIST @{qname(database, schema, stage)}")
                all_listed = cur.fetchall() or []
                logger.info("LIST total under stage=%s", len(all_listed))
                if all_listed:
                    for row in all_listed[:10]:
                        logger.info("LIST sample: %s", row[0])

                cur.execute(f"LIST @{qname(database, schema, stage)} PATTERN='{pattern}'")
                listed = cur.fetchall() or []
                logger.info("LIST matched %s file(s)", len(listed))
                if listed:
                    for row in listed[:10]:
                        logger.info("LIST file: %s", row[0])

                copy_sql = f"""
                    COPY INTO {qname(database, schema, raw_lines_table)} (filename, file_row_number, line)
                    FROM (
                      SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
                      FROM @{qname(database, schema, stage)} (PATTERN => '{pattern}')
                    )
                    ON_ERROR = CONTINUE
                    FORCE = {'TRUE' if force_copy else 'FALSE'}
                """

                logger.info("COPY into RAW %s.%s.%s (FORCE=%s)", database, schema, raw_lines_table, force_copy)
                cur.execute(copy_sql)

                loaded_lines = copy_result_rows_loaded(cur)
                logger.info("COPY loaded %s line(s) into RAW", loaded_lines)

                cur.execute(
                    f"UPDATE {qname(database, schema, raw_lines_table)} SET "
                    "processed = COALESCE(processed, FALSE), "
                    "ingested_ts = COALESCE(ingested_ts, CURRENT_TIMESTAMP()) "
                    "WHERE processed IS NULL OR ingested_ts IS NULL"
                )

                dataset_expr = f"SPLIT_PART(r.filename, '/', -{dataset_level})"

                insert_sql = f"""
                    INSERT INTO {qname(database, schema, target_table)}
                      (dataset, filename, file_row_number, feature_index, geometry_type, feature, properties, geom, geom_geography, geom_geometry)
                    SELECT
                      {dataset_expr} AS dataset,
                      r.filename,
                      r.file_row_number,
                      0 AS feature_index,
                      feat:geometry:type::string AS geometry_type,
                      feat AS feature,
                      COALESCE(feat:properties, OBJECT_CONSTRUCT()) AS properties,
                      feat:geometry AS geom,
                      TRY_TO_GEOGRAPHY(feat:geometry, TRUE) AS geom_geography,
                      TRY_TO_GEOMETRY(feat:geometry) AS geom_geometry
                    FROM (
                      SELECT
                        filename,
                        file_row_number,
                        TRY_PARSE_JSON(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(line, '\\r$', '')), '^\\s*,', '')) AS feat
                      FROM {qname(database, schema, raw_lines_table)}
                      WHERE COALESCE(processed, FALSE) = FALSE
                    ) r
                    WHERE r.feat IS NOT NULL
                      AND r.feat:type::string = 'Feature'
                      AND NOT EXISTS (
                        SELECT 1
                          FROM {qname(database, schema, target_table)} t
                         WHERE t.filename = r.filename
                           AND t.file_row_number = r.file_row_number
                           AND t.feature_index = 0
                      )
                """

                logger.info("INSERT into FINAL %s.%s.%s", database, schema, target_table)
                cur.execute(insert_sql)
                inserted = cur.rowcount if cur.rowcount is not None else 0
                logger.info("Inserted %s feature row(s)", inserted)

                cur.execute(
                    f"UPDATE {qname(database, schema, raw_lines_table)} "
                    "SET processed = TRUE, ingested_ts = COALESCE(ingested_ts, CURRENT_TIMESTAMP()) "
                    "WHERE COALESCE(processed, FALSE) = FALSE"
                )

                cur.execute(
                    f"UPDATE {qname(database, schema, target_table)} "
                    "SET load_ts = COALESCE(load_ts, CURRENT_TIMESTAMP()) "
                    "WHERE load_ts IS NULL"
                )

                cur.execute(f"SELECT COUNT(*) FROM {qname(database, schema, raw_lines_table)}")
                raw_cnt = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(*) FROM {qname(database, schema, target_table)}")
                tgt_cnt = cur.fetchone()[0]
                logger.info("Counts: RAW_LINES=%s FINAL=%s", raw_cnt, tgt_cnt)

                try:
                    build_dataset_tables(cur, cfg, database, schema, target_table)
                except Exception as e:
                    logger.exception("Failed to build per-dataset tables: %s", e)
                    if not bool(cfg.get("continue_on_error", True)):
                        raise

            finally:
                try:
                    cur.close()
                except Exception:
                    pass
                conn.close()


if __name__ == "__main__":
    main()