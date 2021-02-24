from .path import Path


def is_delta_lake(path: Path) -> bool:
    return path.dir_name.endswith('_delta_log') \
           and path.extension in {'json', 'parquet'}


def has_extension(extension: str):
    def _validation(path: Path) -> bool:
        return path.extension.lower() == extension
