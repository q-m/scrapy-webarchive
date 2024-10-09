from pathlib import Path

tests_datadir = str(Path(__file__).parent.resolve() / "data")

def get_test_data_path(*paths: str) -> Path:
    """Return test data"""

    return Path(tests_datadir, *paths)
