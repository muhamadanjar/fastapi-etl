from pathlib import Path
from typing import List

def get_all_files(directory: str, extensions: List[str] = None) -> List[str]:
    files = []
    for path in Path(directory).rglob("*"):
        if path.is_file() and (not extensions or path.suffix in extensions):
            files.append(str(path))
    return files
