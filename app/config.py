import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def environment(name: str) -> Optional[str]:
    """Read one environment variable after loading the project .env once."""
    return os.getenv(name)
