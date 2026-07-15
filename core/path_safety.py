from pathlib import Path
from typing import Union


PathLike = Union[str, Path]


def resolve_existing_file_within(path: PathLike, root: PathLike) -> Path | None:
    """Resolve an existing file and ensure its real path is contained by root."""
    try:
        resolved_root = Path(root).resolve(strict=True)
        resolved_path = Path(path).resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return None

    if not resolved_path.is_file() or not resolved_path.is_relative_to(resolved_root):
        return None
    return resolved_path
