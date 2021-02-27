from .path import Path


def is_delta_lake(path: Path) -> bool:
    return path.dir_name.endswith('_delta_log') \
           and path.extension in {'json', 'parquet'}


def has_extension(extension: str):
    def _validation(path: Path) -> bool:
        return path.extension.lower() == extension
    return _validation


def is_hadoop_hidden(path: Path) -> bool:
    return path.base_name.startswith('_')
