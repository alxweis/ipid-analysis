from pathlib import Path

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

MODELS_DIR = PROJ_ROOT / "models"

REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

# Record names
IP_ADDR = "IP_ADDR"
IPID_SEQUENCE = "IPID_SEQUENCE"
IPID_SELECTION_STRATEGY = "IPID_SELECTION_STRATEGY"

# File names
IPID_MEASURE_NAME = "ipid.pq"
IPID_CONFIG_SNAPSHOT_NAME = "ipid.snapshot.yaml"
IPID_STRATEGY_DATA_NAME = "strategy.pq"
IPID_STRATEGY_DIST_PDF_NAME = "strategy-distribution.pdf"
IPID_STRATEGY_DIST_JSON_NAME = "strategy-distribution.json"

# If tqdm is installed, configure loguru with tqdm.write
# https://github.com/Delgan/loguru/issues/135
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass
