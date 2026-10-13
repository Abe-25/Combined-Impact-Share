import os
import yaml
import socket
import streamlit as st
from typing import Dict, List, Any, Tuple, Optional

st.set_page_config(
    page_title="Smart Integration Builder | CGI",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="auto",
)


# =====================================================================
# MODULE: CONFIGURATION & INGESTION SCRIPT GENERATOR
# =====================================================================
def render_config_generator_tab():
    st.write("Fill in the fields below to generate and download your pipeline configuration file and ingestion code.")

    # Initialize session state
    if 'config_values' not in st.session_state:
        st.session_state.config_values = {}
    if 'generated_yaml' not in st.session_state:
        st.session_state.generated_yaml = None
    if 'generated_script' not in st.session_state:
        st.session_state.generated_script = None

    if st.button("Generate Sample"):
        config_path = "config.yml"
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                    if config and 'dev' in config:
                        st.session_state.config_values = config.get('dev', {})
                        st.success(f"Loaded sample configuration from '{config_path}'.")
                    else:
                        st.session_state.config_values = {}
                        st.warning(f"Could not find 'dev' configuration in '{config_path}'.")
            except Exception as e:
                st.error(f"Failed to load or parse '{config_path}': {e}")
        else:
            st.warning(f"Sample file '{config_path}' not found in current directory.")

    # Use a helper to get values from session_state, with nesting support
    def get_val(key_path: str, default: Any = "") -> Any:
        keys = key_path.split('.')
        val = st.session_state.get('config_values', {})
        for key in keys:
            if val is None: return default
            if isinstance(val, dict):
                val = val.get(key)
            elif isinstance(val, list) and key.isdigit() and int(key) < len(val):
                val = val[int(key)]
            else:
                return default
        return val if val is not None else default

    with st.form("config_form"):
        st.subheader("Dev Configuration")
        key_vault = st.text_input("Key Vault URL / Identifier", value=get_val('key_vault'))

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 📦 Azure Blob Storage")
            blob_secret_name = st.text_input("Secret Name", key="blob_secret", value=get_val('blob_storage.secret_name'))
            blob_container_name = st.text_input("Container Name*", value=get_val('blob_storage.container_name'))
            blob_base_prefix = st.text_input("Base Prefix", value=get_val('blob_storage.base_prefix'))
            blob_target_prefix = st.text_input("Target Prefix", value=get_val('blob_storage.target_prefix'))
            overwrite_val = get_val('blob_storage.overwrite_geojson', True)
            blob_overwrite_geojson = st.selectbox("Overwrite GeoJSON", [True, False], index=0 if overwrite_val else 1)
            continue_val = get_val('blob_storage.continue_on_error', False)
            blob_continue_on_error = st.selectbox("Continue on Error", [True, False], index=0 if continue_val else 1)

            st.markdown("#### 📥 Ingestion & Processing")
            ingestion_target_epsg = st.text_input("Target EPSG", value=str(get_val('ingestion.target_epsg', '')))
            include_layers_list = get_val('ingestion.include_layers', [])
            ingestion_include_layers = st.text_input("Include Layers (comma-separated)", value=", ".join(map(str, include_layers_list)))
            exclude_layers_list = get_val('ingestion.exclude_layers', [])
            ingestion_exclude_layers = st.text_input("Exclude Layers (comma-separated)", value=", ".join(map(str, exclude_layers_list)))
            nonspatial_val = get_val('ingestion.include_nonspatial_tables', True)
            ingestion_include_nonspatial_tables = st.selectbox("Include Non-spatial Tables", [True, False], index=0 if nonspatial_val else 1)
            load_pattern_ext = st.text_input("Pattern Ext (e.g. .csv)", value=get_val('load.pattern_ext'))

        with col2:
            st.markdown("#### ❄️ Snowflake Configuration")
            sf_name = st.text_input("Name*", value=get_val('snowflake.0.name'))
            sf_account = st.text_input("Account*", value=get_val('snowflake.0.account'))
            sf_database = st.text_input("Database*", value=get_val('snowflake.0.database'))
            sf_schema = st.text_input("Schema*", value=get_val('snowflake.0.schema'))
            sf_stage = st.text_input("Stage*", value=get_val('snowflake.0.stage'))
            sf_user = st.text_input("User*", value=get_val('snowflake.0.user'))
            sf_private_key_secret_name = st.text_input("Private Key Secret Name", key="sf_secret", value=get_val('snowflake.0.private_key.secret_name'))
            sf_role = st.text_input("Role*", value=get_val('snowflake.0.role'))
            sf_warehouse = st.text_input("Warehouse*", value=get_val('snowflake.0.warehouse'))
            sf_storage_integration = st.text_input("Storage Integration*", value=get_val('snowflake.0.storage_integration'))
            sf_term_target_table = st.text_input("Term Target Table", value=get_val('snowflake.0.term_target_table'))
            sf_pipe_target_table = st.text_input("Pipe Target Table", value=get_val('snowflake.0.pipe_target_table'))
            sf_load_mode = st.text_input("Load Mode", value=get_val('snowflake.0.load_mode'))
            sf_dataset_level = st.text_input("Dataset Level", value=str(get_val('snowflake.0.dataset_level', '')))
            default_schema = "ID VARCHAR(255), METADATA VARIANT, PAYLOAD VARIANT, INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
            sf_table_schema = st.text_area(
                "Target Table Schema (Column Definitions)",
                value=get_val('snowflake.0.table_schema'),
                help="SQL column definitions used to auto-create the Snowflake table if it does not exist."
            )

        submitted = st.form_submit_button("Generate YAML Configuration & Scripts", use_container_width=True)

    if submitted:
        # Validation for mandatory fields
        errors = []
        if not blob_container_name: errors.append("Blob: Container Name is required.")
        if not sf_name: errors.append("Snowflake: Name is required.")
        if not sf_account: errors.append("Snowflake: Account is required.")
        if not sf_database: errors.append("Snowflake: Database is required.")
        if not sf_schema: errors.append("Snowflake: Schema is required.")
        if not sf_stage: errors.append("Snowflake: Stage is required.")
        if not sf_user: errors.append("Snowflake: User is required.")
        if not sf_role: errors.append("Snowflake: Role is required.")
        if not sf_warehouse: errors.append("Snowflake: Warehouse is required.")
        if not sf_storage_integration: errors.append("Snowflake: Storage Integration is required.")

        if errors:
            for error in errors:
                st.error(f"**Validation Error:** {error}")
            st.session_state.generated_yaml = None
            st.session_state.generated_script = None
            return

        include_layers = [x.strip() for x in ingestion_include_layers.split(',')] if ingestion_include_layers.strip() else []
        exclude_layers = [x.strip() for x in ingestion_exclude_layers.split(',')] if ingestion_exclude_layers.strip() else []
        target_epsg = int(ingestion_target_epsg) if ingestion_target_epsg.isdigit() else (ingestion_target_epsg if ingestion_target_epsg else None)
        dataset_level = int(sf_dataset_level) if sf_dataset_level.isdigit() else (sf_dataset_level if sf_dataset_level else None)

        config_data = {
            'dev': {
                'key_vault': key_vault,
                'blob_storage': {
                    'secret_name': blob_secret_name,
                    'container_name': blob_container_name,
                    'base_prefix': blob_base_prefix,
                    'target_prefix': blob_target_prefix,
                    'overwrite_geojson': blob_overwrite_geojson,
                    'continue_on_error': blob_continue_on_error
                },
                'ingestion': {
                    'target_epsg': target_epsg,
                    'include_layers': include_layers,
                    'exclude_layers': exclude_layers,
                    'include_nonspatial_tables': ingestion_include_nonspatial_tables
                },
                'load': {
                    'pattern_ext': load_pattern_ext
                },
                'snowflake': [
                    {
                        'name': sf_name,
                        'account': sf_account,
                        'database': sf_database,
                        'schema': sf_schema,
                        'stage': sf_stage,
                        'user': sf_user,
                        'private_key': {
                            'secret_name': sf_private_key_secret_name
                        },
                        'role': sf_role,
                        'warehouse': sf_warehouse,
                        'storage_integration': sf_storage_integration,
                        'term_target_table': sf_term_target_table,
                        'pipe_target_table': sf_pipe_target_table,
                        'load_mode': sf_load_mode,
                        'dataset_level': dataset_level,
                        'table_schema': sf_table_schema.strip() if sf_table_schema.strip() else "ID VARCHAR(255), METADATA VARIANT, PAYLOAD VARIANT, INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
                    }
                ]
            }
        }

        yaml_str = yaml.dump(config_data, default_flow_style=False, sort_keys=False)
        st.session_state.generated_yaml = yaml_str

        # --- Script Generation ---

        py_script_base = '''import os
import sys
import logging
from typing import Dict, Any, Optional, List, Tuple
import yaml
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import AzureError, ResourceNotFoundError, ClientAuthenticationError
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
except ImportError:
    serialization = None
    logging.warning("Cryptography library not found. Private key authentication for Snowflake will be disabled.")

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ProductionIngestion")


def load_config(config_path: str = "config.yml", env_target: str = "dev") -> Dict[str, Any]:
    """Loads and validates the YAML configuration file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file '{config_path}' not found.")
    with open(config_path, "r", encoding="utf-8") as f:
        full_config = yaml.safe_load(f)
    if env_target not in full_config:
        raise KeyError(f"Environment '{env_target}' not found in config file.")
    logger.info(f"Successfully loaded configuration for '{env_target}'.")
    return full_config[env_target]


def get_secret(secret_name: Optional[str], key_vault_url: Optional[str] = None) -> Optional[str]:
    """Retrieves a secret from Azure Key Vault or an environment variable."""
    if not secret_name:
        return None
    if key_vault_url:
        try:
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=key_vault_url, credential=credential)
            return client.get_secret(secret_name).value
        except Exception as e:
            logger.warning(f"Could not get secret '{secret_name}' from Key Vault: {e}. Trying environment variable.")
    
    env_val = os.environ.get(secret_name)
    if not env_val:
        logger.warning(f"Secret '{secret_name}' not found in Key Vault or environment variables.")
    return env_val


def get_snowflake_connection(sf_cfg: Dict[str, Any], key_vault_url: Optional[str]) -> snowflake.connector.SnowflakeConnection:
    """Establishes and returns a Snowflake connection."""
    pk_secret_name = sf_cfg.get("private_key", {}).get("secret_name")
    private_key_pem = get_secret(pk_secret_name, key_vault_url)
    
    private_key_bytes = None
    if private_key_pem and serialization:
        try:
            p_key = serialization.load_pem_private_key(
                private_key_pem.encode(),
                password=None,
                backend=default_backend()
            )
            private_key_bytes = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        except Exception as e:
            logger.error(f"Failed to parse private key: {e}")
            raise

    logger.info(f"Connecting to Snowflake account: {sf_cfg['account']}")
    conn = snowflake.connector.connect(
        account=sf_cfg['account'],
        user=sf_cfg['user'],
        database=sf_cfg['database'],
        schema=sf_cfg['schema'],
        warehouse=sf_cfg['warehouse'],
        role=sf_cfg['role'],
        private_key=private_key_bytes
    )
    logger.info("Snowflake connection successful.")
    return conn


def list_blobs(blob_cfg: Dict[str, Any], key_vault_url: Optional[str]) -> Tuple[Optional[ContainerClient], List[str]]:
    """Connects to Azure Blob Storage and lists blobs matching a prefix."""
    conn_str = get_secret(blob_cfg['secret_name'], key_vault_url)
    if not conn_str:
        raise ValueError("Azure Blob Storage connection string is missing.")
        
    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client(blob_cfg['container_name'])
        if not container_client.exists():
            raise ResourceNotFoundError(f"Container '{blob_cfg['container_name']}' does not exist.")
    except Exception as e:
        logger.error(f"Failed to connect to Azure Blob Storage: {e}")
        return None, []

    prefix = blob_cfg.get('target_prefix') or blob_cfg.get('base_prefix', '')
    logger.info(f"Listing blobs in container '{blob_cfg['container_name']}' with prefix '{prefix}'...")
    blob_list = [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]
    logger.info(f"Found {len(blob_list)} blobs.")
    return container_client, blob_list


def process_file_in_snowflake(
    conn: snowflake.connector.SnowflakeConnection,
    sf_cfg: Dict[str, Any],
    blob_name: str,
    file_type: str,
):
    """
    Processes a single file from blob storage and loads it into Snowflake,
    handling schema evolution by adding new columns if necessary.
    """
    db = sf_cfg['database']
    sc = sf_cfg['schema']
    stage = sf_cfg['stage']
    target_table = sf_cfg.get('pipe_target_table') or sf_cfg.get('term_target_table')
    table_schema_def = sf_cfg.get('table_schema', 'ID VARCHAR')

    if not target_table:
        logger.warning(f"No target table specified for Snowflake config '{sf_cfg.get('name')}'. Skipping file {blob_name}.")
        return

    cursor = conn.cursor()
    try:
        logger.info(f"Processing file: {blob_name} -> {db}.{sc}.{target_table}")
        
        # 1. Create dynamic file format for CSV or JSON
        file_type_upper = file_type.upper()
        ff_name = f"temp_ff_{file_type_upper}"
        
        if file_type_upper == 'CSV':
            create_ff_sql = f"CREATE OR REPLACE FILE FORMAT {db}.{sc}.{ff_name} TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\\"' SKIP_HEADER = 1 PARSE_HEADER = TRUE;"
        elif file_type_upper == 'JSON':
            create_ff_sql = f"CREATE OR REPLACE FILE FORMAT {db}.{sc}.{ff_name} TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE;"
        else:
            logger.warning(f"Unsupported file type '{file_type}'. Skipping {blob_name}.")
            return
        cursor.execute(create_ff_sql)
        logger.info(f"Created/Replaced file format '{ff_name}'.")

        # 2. Check if table exists and create if not
        cursor.execute(f"SHOW TABLES LIKE '{target_table}' IN SCHEMA {db}.{sc}")
        if not cursor.fetchone():
            logger.info(f"Table '{target_table}' not found. Creating with schema from config.")
            # Use the schema from config.yml to create the table initially
            create_table_sql = f"CREATE TABLE {db}.{sc}.{target_table} ({table_schema_def});"
            cursor.execute(create_table_sql)
            logger.info(f"Table '{target_table}' created.")

        # 3. Infer schema from the staged file to detect new columns
        staged_file_path = f"@{stage}/{blob_name}"
        infer_sql = f"SELECT COLUMN_NAME FROM TABLE(INFER_SCHEMA(LOCATION=>'{staged_file_path}', FILE_FORMAT=>'{ff_name}'));"
        
        try:
            cursor.execute(infer_sql)
            inferred_columns = {row[0] for row in cursor.fetchall()}
            logger.info(f"Inferred columns from file: {', '.join(inferred_columns)}")
        except Exception as e:
            logger.error(f"Could not infer schema for file {blob_name}: {e}")
            return

        # 4. Get existing table schema
        cursor.execute(f"DESCRIBE TABLE {db}.{sc}.{target_table};")
        existing_columns = {row[0] for row in cursor.fetchall()}
        logger.info(f"Existing columns in table: {', '.join(existing_columns)}")

        # 5. Find new columns and add them to the table
        new_columns = inferred_columns - existing_columns
        if new_columns:
            logger.info(f"New columns detected: {', '.join(new_columns)}")
            for col_name in new_columns:
                # Add new columns as VARCHAR. This is a safe default.
                # Snowflake's COPY with MATCH_BY_COLUMN_NAME will handle casting.
                alter_sql = f'ALTER TABLE {db}.{sc}.{target_table} ADD COLUMN "{col_name}" VARCHAR;'
                try:
                    cursor.execute(alter_sql)
                    logger.info(f"Added column '{col_name}' to table '{target_table}'.")
                except Exception as e:
                    logger.error(f"Failed to add column '{col_name}': {e}")
        else:
            logger.info("No new columns detected. Schema is up to date.")

        # 6. Copy data into the table, matching columns by name
        copy_sql = f"""
        COPY INTO {db}.{sc}.{target_table}
        FROM '{staged_file_path}'
        FILE_FORMAT = (FORMAT_NAME = '{ff_name}')
        MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
        ON_ERROR = 'CONTINUE';
        """
        logger.info(f"Executing COPY command for {blob_name}...")
        cursor.execute(copy_sql)
        logger.info(f"Successfully loaded data from {blob_name} into {target_table}.")

    except Exception as e:
        logger.error(f"An error occurred while processing {blob_name}: {e}")
    finally:
        cursor.close()


def main():
    """Main execution function for the ingestion pipeline."""
    logger.info("=== Starting Data Ingestion Pipeline ===")
    try:
        config = load_config("config.yml", env_target="dev")
    except Exception as e:
        logger.critical(f"Pipeline aborted. Failed to load config: {e}")
        sys.exit(1)

    key_vault_url = config.get("key_vault")
    blob_cfg = config.get("blob_storage", {})
    sf_configs = config.get("snowflake", [])
    continue_on_error = blob_cfg.get("continue_on_error", False)

    try:
        _, blobs = list_blobs(blob_cfg, key_vault_url)
    except Exception as e:
        logger.critical(f"Blob Storage validation failed: {e}")
        if not continue_on_error:
            sys.exit(1)
        blobs = []

    if not blobs:
        logger.info("No blobs found to process. Exiting.")
        sys.exit(0)

    file_ext = (config.get('load', {}).get('pattern_ext') or '.csv').replace('.', '').lower()

    for sf_cfg in sf_configs:
        conn = None
        try:
            conn = get_snowflake_connection(sf_cfg, key_vault_url)
            for blob_name in blobs:
                if blob_name.lower().endswith(file_ext):
                    process_file_in_snowflake(conn, sf_cfg, blob_name, file_ext)
                else:
                    logger.info(f"Skipping blob {blob_name} as it does not match extension '{file_ext}'.")
        except Exception as e:
            logger.error(f"Failed processing for Snowflake target '{sf_cfg.get('name')}': {e}")
            if not continue_on_error:
                sys.exit(1)
        finally:
            if conn:
                conn.close()
                logger.info(f"Snowflake connection closed for target '{sf_cfg.get('name')}'.")

    logger.info("=== Data Ingestion Pipeline Execution Complete ===")


if __name__ == "__main__":
    main()
'''
        additional_scripts_content = []
        scripts_folder_name = "Script_Fol"
        script_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_path = os.path.join(script_dir, scripts_folder_name)
        if not os.path.isdir(scripts_path):
            scripts_path = os.path.join(os.getcwd(), scripts_folder_name)

        if os.path.isdir(scripts_path):
            st.info(f"Scanning for additional scripts in '{scripts_folder_name}/'...")
            for filename in sorted(os.listdir(scripts_path)):
                if filename.endswith(".py"):
                    filepath = os.path.join(scripts_path, filename)
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            content = f.read()
                        additional_scripts_content.append(f"\n\n# ========= From: {filename} =========\n{content}")
                        st.caption(f"Found and appended logic from `{filename}`.")
                    except Exception as e:
                        st.warning(f"Could not read file {filename}: {e}")

        py_script = py_script_base + "".join(additional_scripts_content)
        st.session_state.generated_script = py_script

        # --- Write files and show success messages on submit ---
        try:
            with open("config.yml", "w", encoding="utf-8") as f:
                f.write(st.session_state.generated_yaml)
            st.success("✅ Configuration successfully generated and saved to 'config.yml' in the execution folder.")
        except Exception as e:
            st.error(f"Failed to write 'config.yml': {e}")

        try:
            with open("ingestion.py", "w", encoding="utf-8") as f:
                f.write(st.session_state.generated_script)
            st.success("✅ Ingestion script generated and saved to 'ingestion.py'.")
        except Exception as e:
            st.error(f"Failed to write 'ingestion.py': {e}")

    # --- Display generated content and download buttons (persists across reruns) ---
    if st.session_state.generated_yaml:
        st.subheader("Config Preview")
        st.code(st.session_state.generated_yaml, language="yaml")
        st.download_button(
            label="⬇️ Download config.yml",
            data=st.session_state.generated_yaml,
            file_name="config.yml",
            mime="text/yaml"
        )

    if st.session_state.generated_script:
        st.divider()
        st.subheader("🐍 Generated Ingestion Script")
        st.code(st.session_state.generated_script, language="python")
        st.download_button(
            label="⬇️ Download ingestion.py",
            data=st.session_state.generated_script,
            file_name="ingestion.py",
            mime="text/x-python"
        )


# =====================================================================
# MAIN APPLICATION ROUTING
# =====================================================================
def get_shareable_link(port: int = 8501) -> str:
    """Resolves local host IP to create a shareable link for others on the network/VPN."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            return f"http://{ip}:{port}"
    except Exception:
        return f"http://localhost:{port}"


def main():
    # ── CGI Visual Identity & Design System CSS ──────────────────────────────────
    st.markdown("""
    <style>
    /* ── CGI Brand Color Tokens ─────────────────────────────────────────── */
    :root {
      --cgi-purple: #5236AB;
      --cgi-purple-900: #200A58;
      --cgi-purple-800: #2D1E5E;
      --cgi-purple-700: #3A2679;
      --cgi-purple-500: #755EBC;
      --cgi-purple-400: #9E83F5;
      --cgi-purple-300: #AFA3D8;
      --cgi-purple-200: #CBC3E6;
      --cgi-purple-100: #E6E3F3;
      --cgi-purple-50: #F2F1F9;

      --cgi-red: #E31937;
      --cgi-red-900: #600A17;
      --cgi-red-800: #7D0D1E;
      --cgi-red-700: #A21127;
      --cgi-red-600: #CF1632;
      --cgi-red-400: #E9465F;
      --cgi-red-300: #ED6479;
      --cgi-red-200: #F395A3;
      --cgi-red-100: #F7B7C1;
      --cgi-red-50: #FCE8EB;

      --cgi-gray-900: #151515;
      --cgi-gray-800: #1C1C1C;
      --cgi-gray-700: #242424;
      --cgi-gray-600: #2E2E2E;
      --cgi-gray: #333333;
      --cgi-gray-400: #5C5C5C;
      --cgi-gray-300: #767676;
      --cgi-gray-200: #A8A8A8;
      --cgi-gray-100: #C0C0C0;
      --cgi-gray-50: #EFEFEF;

      --cgi-white: #FFFFFF;
      --cgi-white-700: #EBEEF2;
      --cgi-white-600: #F0F3F6;
      --cgi-white-500: #F2F4F7;
      --cgi-white-400: #F5F7F9;
      --cgi-white-300: #F6F8F9;
      --cgi-white-200: #F9FAFB;
      --cgi-white-100: #FCFDFD;

      --cgi-success: #1AB977;
      --cgi-success-900: #0B4E32;
      --cgi-success-800: #0E6641;
      --cgi-success-700: #128354;
      --cgi-success-600: #18A86C;
      --cgi-success-400: #48C792;
      --cgi-success-200: #96DFC0;
      --cgi-success-100: #B8E9D5;
      --cgi-success-50: #E8F8F1;

      --cgi-warning: #F1A425;
      --cgi-warning-900: #654510;
      --cgi-warning-800: #855A14;
      --cgi-warning-700: #AB741A;
      --cgi-warning-600: #DB9522;
      --cgi-warning-200: #F9D59B;
      --cgi-warning-100: #FBE3BB;
      --cgi-warning-50: #FEF6E9;

      --cgi-error: #B00020;
      --cgi-error-900: #4A000D;
      --cgi-error-800: #610012;
      --cgi-error-700: #7D0017;
      --cgi-error-600: #A0001D;
      --cgi-error-200: #DB8A98;
      --cgi-error-100: #E7B0BA;
      --cgi-error-50: #F7E6E9;

      --cgi-magenta: #A82465;
      --cgi-gradient-h: linear-gradient(90deg, #E31937 0%, #A82465 60%, #5236AB 100%);
    }

    /* Global Font & Body */
    html, body, [class*="css"] {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      color: var(--cgi-gray);
    }

    /* ── Branded Top Gradient Bar ────────────────────────────────────────── */
    .cgi-brand-bar {
      height: 6px;
      width: 100%;
      background: var(--cgi-gradient-h);
      border-radius: 3px;
      margin-bottom: 1.2rem;
    }

    /* ── Standalone CGI Logo ─────────────────────────────────────────────── */
    .cgi-standalone-logo-container {
      display: flex;
      align-items: center;
      margin-bottom: 0.85rem;
      padding: 0.1rem 0;
    }

    .cgi-standalone-logo {
      background: var(--cgi-red);
      color: #FFFFFF;
      font-weight: 900;
      font-size: 1.35rem;
      letter-spacing: 0.12em;
      padding: 0.4rem 0.95rem;
      border-radius: 6px;
      line-height: 1;
      box-shadow: 0 2px 5px rgba(227, 25, 55, 0.28);
      display: inline-block;
    }

    /* ── Branded Header Card ─────────────────────────────────────────────── */
    .cgi-header-banner {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 1.1rem 1.5rem;
      background: linear-gradient(135deg, #FFFFFF 0%, #F9FAFB 100%);
      border: 1px solid var(--cgi-white-700);
      border-left: 6px solid var(--cgi-purple);
      border-radius: 8px;
      margin-bottom: 1.5rem;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
    }

    .cgi-header-title-text h1 {
      margin: 0 !important;
      padding: 0 !important;
      font-size: 1.65rem !important;
      font-weight: 700 !important;
      color: var(--cgi-purple-900) !important;
      line-height: 1.2 !important;
    }

    .cgi-header-title-text p {
      margin: 0.25rem 0 0 0 !important;
      font-size: 0.9rem !important;
      color: var(--cgi-gray-400) !important;
      font-weight: 500 !important;
    }

    .cgi-stage-badge {
      background: var(--cgi-purple-50);
      color: var(--cgi-purple);
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      padding: 0.4rem 0.85rem;
      border-radius: 20px;
      border: 1.5px solid var(--cgi-purple-200);
      text-transform: uppercase;
    }

    /* ── Typography & Headings ───────────────────────────────────────────── */
    h1, h2, h3, [data-testid="stHeadingWithActionElements"] h1, [data-testid="stHeadingWithActionElements"] h2, [data-testid="stHeadingWithActionElements"] h3 {
      color: var(--cgi-purple-900) !important;
      font-weight: 700 !important;
      letter-spacing: -0.01em !important;
    }

    h4, h5, h6 {
      color: var(--cgi-gray-700) !important;
      font-weight: 600 !important;
    }

    /* ── Sidebar Theming ─────────────────────────────────────────────────── */
    [data-testid="stSidebar"] {
      background-color: var(--cgi-white-200) !important;
      border-right: 1px solid var(--cgi-white-700) !important;
    }

    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
      color: var(--cgi-purple-900) !important;
      border-bottom: 2px solid var(--cgi-purple-100);
      padding-bottom: 0.4rem;
      margin-top: 0.6rem;
    }

    [data-testid="stSidebar"] label {
      color: var(--cgi-gray-700) !important;
      font-weight: 600 !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── Top Brand Gradient Bar & Standalone Logo & Header ────────────────────────
    st.markdown("""
    <div class="cgi-brand-bar"></div>
    <div class="cgi-standalone-logo-container">
      <div class="cgi-standalone-logo">CGI</div>
    </div>
    <div class="cgi-header-banner">
      <div class="cgi-header-title-text">
        <h1>Smart Integration Builder</h1>
        <p>Configuration & Ingestion Script Generator</p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    share_url = get_shareable_link()
    with st.sidebar:
        st.markdown("### 🔗 Share Application")
        st.info("Colleagues on the same network/VPN can access this app at:")
        st.code(share_url, language="text")

    render_config_generator_tab()


if __name__ == "__main__":
    main()