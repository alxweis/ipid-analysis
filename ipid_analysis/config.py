import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import yaml
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

# Raw data directories
ZMAP_DATA_DIR = RAW_DATA_DIR / "zmap"
OS_DATA_DIR = RAW_DATA_DIR / "os"
IPID_DATA_DIR = RAW_DATA_DIR / "ipid"

# Record names
IP_ADDR = "IP_ADDR"
IPID_SEQUENCE = "IPID_SEQUENCE"
IPID_SELECTION_STRATEGY = "IPID_SELECTION_STRATEGY"

# File names
IPID_MEASURE_NAME = "ipid.pq"
IPID_CONFIG_SNAPSHOT_NAME = "ipid.snapshot.yaml"

ZMAP_MEASURE_NAME = "zmap.pq"

STRATEGY_DATA_NAME = "strategy.pq"

STRATEGY_DIST_PDF_NAME = "strategy-distribution.pdf"
STRATEGY_DIST_JSON_NAME = "strategy-distribution.json"

COVERAGE_JSON_NAME = "coverage.json"

# If tqdm is installed, configure loguru with tqdm.write
# https://github.com/Delgan/loguru/issues/135
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass


@dataclass(frozen=True)
class MeasurementID:
    protocol: str
    port: int | None
    timestamp: datetime

    def __str__(self) -> str:
        if self.port is None:
            prefix = self.protocol
        else:
            prefix = f"{self.protocol}-{self.port}"

        return f"{prefix}_{self.timestamp:%Y-%m-%d_%H-%M-%S}"


_MEASUREMENT_ID_RE = re.compile(
    r"^(?:(icmp)|(tcp|udp)-([0-9]{1,5}))_"
    r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})$"
)


def load_measurement_id(value: str) -> MeasurementID:
    match = _MEASUREMENT_ID_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"Invalid measurement ID: {value!r}")

    icmp, protocol, port_str, timestamp_str = match.groups()

    if icmp is not None:
        protocol = "icmp"
        port = None
    else:
        assert protocol is not None
        assert port_str is not None

        port = int(port_str)
        if not (0 <= port <= (1 << 16) - 1):
            raise ValueError(f"Invalid port: {port}")

    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")

    return MeasurementID(
        protocol=protocol,
        port=port,
        timestamp=timestamp,
    )


@dataclass(frozen=True)
class MeasurementConfig:
    zmap: MeasurementID
    connection_count: int
    requests_per_connection: int
    request_ip_ids: np.ndarray  # int64

    @property
    def sequence_length(self) -> int:
        return self.connection_count * self.requests_per_connection


def load_config(snapshot_path: Path) -> MeasurementConfig:
    with snapshot_path.open() as fh:
        data = yaml.safe_load(fh)
    try:
        return MeasurementConfig(
            zmap=load_measurement_id(data["zmap"]),
            connection_count=int(data["connection_count"]),
            requests_per_connection=int(data["requests_per_connection"]),
            request_ip_ids=np.asarray(data["request_ip_ids"], dtype=np.int64),
        )
    except (KeyError, TypeError) as exc:
        raise ValueError(f"{snapshot_path}: missing/invalid measurement fields ({exc})") from exc
