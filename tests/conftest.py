import sys
from pathlib import Path

# Makes the pipeline modules importable without a pytest.ini at the repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
