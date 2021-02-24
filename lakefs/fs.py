import datetime
from typing import Union, Tuple

from pyarrow import NativeFile, BufferReader
from pyarrow.fs import PyFileSystem, FileInfo, FileType, FileSystemHandler, FileSelector

from bravado.exception import HTTPNotFound

from lakefs import Client

LAKEFS_TYPE_NAME = 'lakefs'


def pyarrow_fs(client: Client, repository: str, ref: str):
    """
    A wrapper that returns a pyarrow.fs.PyFileSystem from the LakeFSFileSystem implementation.
    """
    return PyFileSystem(LakeFSFileSystem(client, repository, ref))


def get_file_info(path: str, file_type: FileType, size_bytes: int = 0, mtime_ts: int = 0) -> FileInfo:
    """
    Generate a pyarrow.FileInfo object for the given path metadata.
    Used to convert lakeFS statObject/listObjects responses to pyArrow's format
    """
    return FileInfo(
        path=path,
        type=file_type,
        size=size_bytes,
        mtime=datetime.datetime.fromtimestamp(mtime_ts),
    )


class LakeFSFileSystem(FileSystemHandler):
    """
    A naive read-only implementation of a PyArrow FileSystem.
    Just enough here to be able to read a ParquetFile and a ParquetDataSet:

    Be warned: the current implementation is naive and will read entire objects into memory.

    Examples:
    >>> import lakefs
    >>> import pyarrow.parquet as pq
    >>>
    >>> client = lakefs.Client('http://localhost:8000', '<lakeFS access key ID>', '<lakeFS secret key>')
    >>> fs = lakefs.get_filesystem(client, 'my-repo-name', 'my-branch')
    >>>
    >>> # Do some schema validation
    >>> schema = pq.read_schema(fs.open_input_file('some_file.parquet'))
    >>> for field in schema:
    >>>     if field.name.startswith('user_'):
    >>>         raise ValueError('user identifying columns are not allowed!')
    >>>
    >>> # read a dataset and explore the data
    >>> dataset = pq.ParquetDataset('collections/events/', filesystem=client.filesystem('my-repo-name', 'my-branch'))
    >>> table = dataset.read_pandas()
    >>> assert len(table) > 50000
    """

    def __init__(self, client: Client, repository: str, ref: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = client
        self.repository = repository
        self.ref = ref

    def copy_file(self, src: str, dst: str):
        pass

    def create_dir(self, path: str, recursive: bool = True):
        pass

    def delete_dir(self, path: str):
        pass

    def delete_dir_contents(self, path: str, accept_root_dir: bool = False):
        pass

    def delete_file(self, path: str):
        pass

    def get_file_info(self, paths_or_selector):
        if isinstance(paths_or_selector, str):
            return self._get_file_info(paths_or_selector)
        return [self._get_file_info(p)for p in paths_or_selector]

    def normalize_path(self, path):
        return path

    def move(self, src: str, dst: str):
        pass

    def open_append_stream(self, path: str, compression: str = 'detect', buffer_size: int = None):
        pass

    def open_input_file(self, source: str, compression: str = 'detect', buffer_size: int = None) -> NativeFile:
        obj = self._client.get_object(self.repository, self.ref, source)
        return BufferReader(obj)

    def open_input_stream(self, source: str, compression: str = 'detect', buffer_size: int = None):
        pass

    def open_output_stream(self, path: str, compression: str = 'detect', buffer_size: int = None):
        pass

    def delete_root_dir_contents(self, path: str, accept_root_dir: bool = False):
        pass

    def get_file_info_selector(self, selector: Union[FileSelector, str, Tuple[str]]):
        delimiter = '/'
        path = selector
        if isinstance(selector, FileSelector):
            path = selector.base_dir
            if selector.recursive:
                delimiter = ''
        entries = list(self._list_entries(path, delimiter))
        return entries

    def get_type_name(self, *args, **kwargs):
        return LAKEFS_TYPE_NAME

    def _get_file_info(self, path) -> FileInfo:
        if path.endswith('/'):
            # Check it exists
            if next(self._list_entries(path, max_amount=1), None):
                # this doesn't exist!
                return get_file_info(path, FileType.NotFound)
            return get_file_info(path, FileType.Directory)
        # get file
        try:
            stat = self._client.stat_object(repository=self.repository, ref=self.ref, path=path)
        except HTTPNotFound:
            return get_file_info(path, FileType.NotFound)  # this doesn't exist!
        return get_file_info(path, FileType.File, stat.size_bytes, stat.mtime)

    def _list_entries(self, path: str, delimiter: str = '/', max_amount: int = None):
        for result in self._client.list(self.repository, self.ref, path, delimiter, max_amount):
            if result.path_type == 'object':
                yield get_file_info(result.path, FileType.File, result.size_bytes, result.mtime)
            else:
                yield get_file_info(result.path, FileType.Directory)
