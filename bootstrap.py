from pathlib import Path
import sys


def add_src_to_path() -> None:
    """
    Ensure the `src/` folder is on sys.path so local modules resolve
    when running scripts directly (python scripts/foo.py).
    """
    root = Path(__file__).resolve().parent
    src = root / "src"
    src_str = str(src)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)
