from __future__ import annotations
import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import snowflake.connector
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

sys.path.append(str(Path(__file__).parent.parent.absolute()))

from utils.ay_akv import AyAzureKeyVault  # noqa: E402

logger = logging.getLogger(__name__)

logging.basicConfig(
    format="%(levelname)s:%(name)s:%(message)s",
    level=logging.INFO,
)


def get_key(cfg: Dict[str, Any], key: str, default=None):
    cur: Any = cfg
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur if cur is not None else default


def required_key(cfg: Dict[str, Any], key: str) -> Any:
    value = get_key(cfg, key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"Missing/empty config key: {key}")
    return value


def storage_account(conn_str: str) -> str:
    match = re.search(r"AccountName=([^;]+)", conn_str)
    if not match:
        raise ValueError("Could not parse AccountName from Azure connection string")
    return match.group(1)


def pem_to_der(pem_text: str) -> bytes:
    private_key = serialization.load_pem_private_key(
        pem_text.encode("utf-8"),
        password=None,
        backend=default_backend(),
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def qident(value: str) -> str:
    value = (value or "").replace('"', '""')
    return f'"{value}"'


def qname(*parts: str) -> str:
    return ".".join(qident(part) for part in parts if part and str(part).strip())


def norm(path_value: str) -> str:
    path_value = (path_value or "").strip().lstrip("/").replace("\\", "/")
    while "//" in path_value:
        path_value = path_value.replace("//", "/")
    return path_value


def pattern(base_prefix: str, processed_subdir: str, ext_regex: str) -> str:
    base = norm(base_prefix).rstrip("/")
    sub = (processed_subdir or "processed").strip().strip("/")
    return rf"^{re.escape(base)}(?:/.*)?/{re.escape(sub)}/.*{ext_regex}$"


def feature_select_sql(raw_table_fqn: str) -> str:
    """
    Returns rows for unprocessed features plus lightweight routing columns.
    """
    return f"""
        WITH src AS (
            SELECT
                filename,
                file_row_number,
                TRY_PARSE_JSON(
                    REGEXP_REPLACE(
                        TRIM(REGEXP_REPLACE(line::STRING, '\\r$', '')),
                        '^\\s*,',
                        ''
                    )
                ) AS feat
            FROM {raw_table_fqn}
            WHERE COALESCE(processed, FALSE) = FALSE
        )
        SELECT
            filename,
            file_row_number,
            feat,
            feat:properties AS props,
            TO_JSON(feat) AS feat_json,
            CASE
                WHEN REGEXP_LIKE(LOWER(filename), 'gtc[_-]?fiberroutes') THEN 'FIBERROUTE'
                WHEN REGEXP_LIKE(LOWER(filename), 'gtc[_-]?datacenters') THEN 'DATACENTER'
                WHEN REGEXP_LIKE(LOWER(filename), 'gtc[_-]?fiberlitbuildings') THEN 'FIBERBUILDING'
                ELSE NULL
            END AS source_layer,
            UPPER(TRIM(COALESCE(feat:properties:TYPE::STRING, ''))) AS type_norm
        FROM src
        WHERE feat IS NOT NULL
          AND feat:type::STRING = 'Feature'
    """


def get_first_column_meta(cur, table_fqn: str) -> Tuple[str, str]:
    """
    Returns (first_column_name, first_column_type) from the existing target table.
    The type lets us build correct MERGE predicates/values for VARIANT vs VARCHAR.
    """
    cur.execute(f"DESC TABLE {table_fqn}")
    rows = cur.fetchall()
    if not rows:
        raise ValueError(f"No columns found in target table: {table_fqn}")
    col_name = rows[0][0]
    col_type = str(rows[0][1]).upper()
    return col_name, col_type


def build_target_exprs(col_name: str, col_type: str) -> Tuple[str, str, str]:
    """
    Returns:
      match_expr: expression to compare existing target row to source feat_json
      value_expr: source expression to insert into target column
      mode: human-readable storage mode for logging
    """
    col_ref = f"t.{qident(col_name)}"
    if "VARIANT" in col_type or "OBJECT" in col_type or "ARRAY" in col_type:
        return f"TO_JSON({col_ref})", "s.src_variant", "VARIANT"
    return col_ref, "s.src_json", "STRING"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev", help="dev/prd")
    ap.add_argument("--run_date", default=None, help="Airflow date (YYYY-MM-DD)")
    ap.add_argument("--force_copy", action="store_true", help="Force Snowflake COPY")
    args = ap.parse_args()

    cfg_path = Path(__file__).with_name("config.yml")
    all_cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    cfg: Dict[str, Any] = all_cfg[(args.env or "dev").lower().strip()]

    vault = f"https://{required_key(cfg, 'key_vault')}.vault.azure.net/"
    kv = AyAzureKeyVault()

    conn_str = kv.get_secret(vault, required_key(cfg, "blob_storage.secret_name"))
    storage_acct = storage_account(conn_str)
    container = required_key(cfg, "blob_storage.container_name")
    base_prefix = get_key(cfg, "blob_storage.folder_prefix") or required_key(
        cfg, "blob_storage.base_prefix"
    )
    processed_subdir = get_key(cfg, "blob_storage.processed_subdir", "processed")
    ext_regex = get_key(cfg, "load.pattern_ext", r"\.geojson$")

    stage_url = f"azure://{storage_acct}.blob.core.windows.net/{container}"
    pat = pattern(str(base_prefix), str(processed_subdir), str(ext_regex))

    logger.info("Stage URL: %s", stage_url)
    logger.info("COPY PATTERN: %s", pat)

    for sf in required_key(cfg, "snowflake"):
        account = required_key(sf, "account")
        database = required_key(sf, "database")
        schema = required_key(sf, "schema")
        stage = required_key(sf, "stage")
        user = required_key(sf, "user")
        role = required_key(sf, "role")
        warehouse = required_key(sf, "warehouse")
        storage_integration = get_key(
            sf, "storage_integration", "ETL_PRODUCT_INTEGRATION_LND"
        )
        raw_table = get_key(sf, "raw_table", "GEOTEL_GEOJSON_RAW")
        fiberroute_table = required_key(sf, "fiberroute_table")
        datacenter_table = required_key(sf, "datacenter_table")
        fiberbuilding_table = required_key(sf, "fiberbuilding_table")
        load_mode = (get_key(sf, "load_mode", "append") or "append").lower().strip()
        force_copy = bool(args.force_copy) or load_mode in {"truncate", "replace"}

        pk_pem = kv.get_secret(vault, required_key(sf, "private_key.secret_name"))
        pk_der = pem_to_der(pk_pem)

        conn: Optional[snowflake.connector.SnowflakeConnection] = None
        cur = None

        raw_table_fqn = qname(database, schema, raw_table)
        fiberroute_fqn = qname(database, schema, fiberroute_table)
        datacenter_fqn = qname(database, schema, datacenter_table)
        fiberbuilding_fqn = qname(database, schema, fiberbuilding_table)
        stage_fqn = qname(database, schema, stage)
        ff_fqn = qname(database, schema, "GEOJSON_LINE_FF")

        try:
            conn = snowflake.connector.connect(
                account=account,
                user=user,
                role=role,
                warehouse=warehouse,
                database=database,
                schema=schema,
                private_key=pk_der,
            )
            conn.autocommit(True)
            cur = conn.cursor()

            cur.execute(f"""
                CREATE OR REPLACE FILE FORMAT {ff_fqn}
                TYPE = CSV
                RECORD_DELIMITER = '\n'
                FIELD_DELIMITER = NONE
                SKIP_HEADER = 0
                TRIM_SPACE = FALSE
                FIELD_OPTIONALLY_ENCLOSED_BY = NONE
                ESCAPE_UNENCLOSED_FIELD = NONE
                NULL_IF = ('')
            """)
            cur.execute(f"""
                CREATE OR REPLACE STAGE {stage_fqn}
                URL = '{stage_url}'
                STORAGE_INTEGRATION = {qident(storage_integration)}
                FILE_FORMAT = {ff_fqn}
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {raw_table_fqn} (
                    filename STRING,
                    file_row_number NUMBER,
                    line VARIANT,
                    ingested_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                    processed BOOLEAN DEFAULT FALSE
                )
            """)

            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {fiberroute_fqn} (RAW_DATA VARIANT)"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {datacenter_fqn} (RAW_DATA VARIANT)"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {fiberbuilding_fqn} (RAW_DATA VARIANT)"
            )

            fiberroute_col, fiberroute_type = get_first_column_meta(cur, fiberroute_fqn)
            datacenter_col, datacenter_type = get_first_column_meta(cur, datacenter_fqn)
            fiberbuilding_col, fiberbuilding_type = get_first_column_meta(
                cur, fiberbuilding_fqn
            )

            fiberroute_match_expr, fiberroute_value_expr, fiberroute_mode = (
                build_target_exprs(fiberroute_col, fiberroute_type)
            )
            datacenter_match_expr, datacenter_value_expr, datacenter_mode = (
                build_target_exprs(datacenter_col, datacenter_type)
            )
            fiberbuilding_match_expr, fiberbuilding_value_expr, fiberbuilding_mode = (
                build_target_exprs(fiberbuilding_col, fiberbuilding_type)
            )

            logger.info(
                "Resolved target storage: FIBERROUTE=%s(%s) DATACENTER=%s(%s) FIBERBUILDING=%s(%s)",
                fiberroute_col,
                fiberroute_mode,
                datacenter_col,
                datacenter_mode,
                fiberbuilding_col,
                fiberbuilding_mode,
            )

            if load_mode in {"truncate", "replace"}:
                logger.info("Load mode '%s' -> truncating target tables", load_mode)
                cur.execute(f"TRUNCATE TABLE {raw_table_fqn}")
                cur.execute(f"TRUNCATE TABLE {fiberroute_fqn}")
                cur.execute(f"TRUNCATE TABLE {datacenter_fqn}")
                cur.execute(f"TRUNCATE TABLE {fiberbuilding_fqn}")

            logger.info("Copying raw GeoJSON lines into %s", raw_table_fqn)
            cur.execute(f"""
                COPY INTO {raw_table_fqn} (filename, file_row_number, line)
                FROM (
                    SELECT
                        METADATA$FILENAME,
                        METADATA$FILE_ROW_NUMBER,
                        $1
                    FROM @{stage_fqn} (PATTERN => '{pat}')
                )
                ON_ERROR = CONTINUE
                FORCE = {'TRUE' if force_copy else 'FALSE'}
            """)
            try:
                cur.fetchall()
            except Exception:
                pass

            common_feature_sql = feature_select_sql(raw_table_fqn)

            fiberroute_match = """
                (
                    f.source_layer = 'FIBERROUTE'
                    OR (
                        f.source_layer IS NULL
                        AND f.props:CARRIER IS NOT NULL
                        AND f.props:CBSA IS NOT NULL
                        AND f.props:LATA IS NOT NULL
                        AND f.props:Length IS NOT NULL
                        AND f.props:REC_ID IS NOT NULL
                        AND f.props:STATE IS NOT NULL
                        AND f.props:Shape_Length IS NOT NULL
                        AND f.props:TYPE IS NOT NULL
                        AND f.props:ZIP IS NOT NULL
                    )
                )
            """

            datacenter_match = """
                (
                    f.source_layer = 'DATACENTER'
                    OR (
                        f.source_layer IS NULL
                        AND f.props:ADDRESS IS NOT NULL
                        AND f.props:BUILDING IS NOT NULL
                        AND f.props:CATEGORY IS NOT NULL
                        AND f.type_norm = 'DATA CENTER'
                    )
                )
            """

            fiberbuilding_match = """
                (
                    f.source_layer = 'FIBERBUILDING'
                    OR (
                        f.source_layer IS NULL
                        AND f.props:ADDRESS IS NOT NULL
                        AND f.props:BUILDING IS NOT NULL
                        AND f.props:CATEGORY IS NOT NULL
                        AND f.type_norm <> 'DATA CENTER'
                    )
                )
            """

            logger.info("Merging FIBERROUTE rows into %s", fiberroute_fqn)
            cur.execute(f"""
                MERGE INTO {fiberroute_fqn} t
                USING (
                    SELECT
                        f.feat AS src_variant,
                        f.feat_json AS src_json,
                        f.feat_json AS feat_json
                    FROM (
                        {common_feature_sql}
                    ) f
                    WHERE {fiberroute_match}
                ) s
                ON {fiberroute_match_expr} = s.feat_json
                WHEN NOT MATCHED THEN
                    INSERT ({qident(fiberroute_col)})
                    VALUES ({fiberroute_value_expr})
            """)

            logger.info("Merging DATACENTER rows into %s", datacenter_fqn)
            cur.execute(f"""
                MERGE INTO {datacenter_fqn} t
                USING (
                    SELECT
                        f.feat AS src_variant,
                        f.feat_json AS src_json,
                        f.feat_json AS feat_json
                    FROM (
                        {common_feature_sql}
                    ) f
                    WHERE {datacenter_match}
                ) s
                ON {datacenter_match_expr} = s.feat_json
                WHEN NOT MATCHED THEN
                    INSERT ({qident(datacenter_col)})
                    VALUES ({datacenter_value_expr})
            """)

            logger.info("Merging FIBERBUILDING rows into %s", fiberbuilding_fqn)
            cur.execute(f"""
                MERGE INTO {fiberbuilding_fqn} t
                USING (
                    SELECT
                        f.feat AS src_variant,
                        f.feat_json AS src_json,
                        f.feat_json AS feat_json
                    FROM (
                        {common_feature_sql}
                    ) f
                    WHERE {fiberbuilding_match}
                ) s
                ON {fiberbuilding_match_expr} = s.feat_json
                WHEN NOT MATCHED THEN
                    INSERT ({qident(fiberbuilding_col)})
                    VALUES ({fiberbuilding_value_expr})
            """)

            logger.info("Marking processed raw rows in %s", raw_table_fqn)
            cur.execute(f"""
                UPDATE {raw_table_fqn} r
                SET processed = TRUE,
                    ingested_ts = COALESCE(ingested_ts, CURRENT_TIMESTAMP())
                WHERE COALESCE(processed, FALSE) = FALSE
                  AND EXISTS (
                      SELECT 1
                      FROM (
                          {common_feature_sql}
                      ) f
                      WHERE f.filename = r.filename
                        AND f.file_row_number = r.file_row_number
                        AND (
                            {fiberroute_match}
                            OR {datacenter_match}
                            OR {fiberbuilding_match}
                        )
                  )
            """)

            cur.execute(f"SELECT COUNT(*) FROM {raw_table_fqn}")
            raw_cnt = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {fiberroute_fqn}")
            fiberroute_cnt = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {datacenter_fqn}")
            datacenter_cnt = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {fiberbuilding_fqn}")
            fiberbuilding_cnt = cur.fetchone()[0]
            logger.info(
                "Counts: RAW=%s FIBERROUTE=%s DATACENTER=%s FIBERBUILDING=%s",
                raw_cnt,
                fiberroute_cnt,
                datacenter_cnt,
                fiberbuilding_cnt,
            )
        finally:
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
