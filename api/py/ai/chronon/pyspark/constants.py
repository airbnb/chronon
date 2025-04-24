from __future__ import annotations

# --------------------------------------------------------------------------
# Company Specific Constants
# --------------------------------------------------------------------------

PARTITION_COLUMN_FORMAT: str = '%Y%m%d'

# --------------------------------------------------------------------------
# Platform Specific Constants
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# Databricks Constants
# --------------------------------------------------------------------------
DATABRICKS_OUTPUT_NAMESPACE: str = 'chronon_poc_usertables'
DATABRICKS_JVM_LOG_FILE: str = "/databricks/chronon_logfile.log"
DATABRICKS_ROOT_DIR_FOR_IMPORTED_FEATURES: str = "src"