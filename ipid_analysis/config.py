import os
from pathlib import Path
import re

from dotenv import load_dotenv
from loguru import logger

# Load environment variables from .env file if it exists
load_dotenv()

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
logger.info(f"PROJ_ROOT path is: {PROJ_ROOT}")

DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"
REFERENCES_DIR = PROJ_ROOT / "references"

MODELS_DIR = PROJ_ROOT / "models"

REPORTS_DIR = PROJ_ROOT / "reports"
RAW_STORAGE_DIR = RAW_DATA_DIR
PROCESSED_STORAGE_DIR = PROCESSED_DATA_DIR
REPORTS_STORAGE_DIR = REPORTS_DIR
SCOPE = os.environ.get("IPID_ANALYSIS_SCOPE", "")
if SCOPE:
    if not re.fullmatch(r"continental/[A-Za-z0-9][A-Za-z0-9_-]{0,79}/(eu|as|af|na|sa|au)", SCOPE):
        raise ValueError("invalid continental analysis scope")
    RAW_DATA_DIR /= SCOPE
    PROCESSED_DATA_DIR /= SCOPE
    REPORTS_DIR /= SCOPE
FIGURES_DIR = REPORTS_DIR / "figures"


def os_source_path(raw_root: Path, measurement_id: str, name: str = "os.pq") -> Path:
    shared = os.environ.get("IPID_ANALYSIS_SHARED_OS_DIR")
    if measurement_id == "shared-os" and SCOPE:
        if not shared:
            raise ValueError("shared OS dataset has not been verified")
        return Path(shared) / name
    return raw_root / "os" / measurement_id / name

# If tqdm is installed, configure loguru with tqdm.write
# https://github.com/Delgan/loguru/issues/135
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass
