import os
import logging
from logging.handlers import RotatingFileHandler

from collector import run_forever


def setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fh = RotatingFileHandler(
        "logs/collector.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))

    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

    root.addHandler(fh)
    root.addHandler(sh)


if __name__ == "__main__":
    setup_logging()
    run_forever()
