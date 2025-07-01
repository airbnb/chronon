from __future__ import annotations
from typing import Optional

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
DATABRICKS_OUTPUT_NAMESPACE: Optional[str] = None
DATABRICKS_JVM_LOG_FILE: str = "/databricks/chronon_logfile.log"
DATABRICKS_ROOT_DIR_FOR_IMPORTED_FEATURES: str = "src"
