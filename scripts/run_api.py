import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bootstrap
bootstrap.add_src_to_path()

import uvicorn

from mb160_service.api.main import app
from mb160_service.config import get_api_settings


def main() -> None:
    settings = get_api_settings()
    uvicorn.run(app, host="0.0.0.0", port=settings.port, reload=True)


if __name__ == "__main__":
    main()
