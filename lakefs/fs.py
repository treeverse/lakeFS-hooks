import datetime
from typing import Union, Tuple

from pyarrow import NativeFile, BufferReader
from pyarrow.fs import PyFileSystem, FileInfo, FileType, FileSystemHandler, FileSelector

from bravado.client import SwaggerClient
from bravado.exception import HTTPNotFound

LISTING_PREFETCH_SIZE = 1000


def pyarrow_fs(client: SwaggerClient, repository: str, ref: str):
    """
    A wrapper that returns a pyarrow.fs.PyFileSystem from the LakeFSFileSystem implementation.
    """
    return PyFileSystem(LakeFSFileSystem(client, repository, ref))


def get_file_info(path: str, ftype: FileType, size_bytes: int = 0, mtime_ts : int = 0) -> FileInfo:
    """
    Generate a pyarrow.FileInfo object for the given path metadata.
    Used to convert lakeFS statObject/listObjects responses to pyArrow's format
    """
    return FileInfo(
        path=path,
        type=ftype,
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
    >>> fs = client.filesystem('my-repo-name', 'my-branch')
    >>>
    >>> # Do some schema validation
    >>> schema = pq.read_schema(fs.open_input_file('some_file.parquet'))
    >>> for field in schema:
    >>>     if field.name.startswith('user_'):
    >>>         raise ValueError('user identifying columns are not allowed!')
    >>>
    >>> # read a dataset and explore the data
    >>> dataset = pq.ParquetDataset('collections/events/', filesystem=c.filesystem('my-repo-name', 'my-branch'))
    >>> table = dataset.read_pandas()
    >>> assert len(table) > 50000
    """

    type_name = 'lakefs'

    def __init__(self, client: SwaggerClient, repository: str, ref: str, *args, **kwargs):
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
        response = self._client.objects.getObject(
            repository=self.repository,
            ref=self.ref,
            path=source).response()
        return BufferReader(response.result)

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
        return self.type_name

    def _get_file_info(self, path) -> FileInfo:
        if path.endswith('/'):
            # Check it exists
            if len(list(self._list_entries(path, max_amount=1))) == 0:
                return get_file_info(path, FileType.NotFound)  # this doesn't exist!
            return get_file_info(path, FileType.Directory)
        # get file
        try:
            response = self._client.objects.statObject(
                repository=self.repository,
                ref=self.ref,
                path=path).response()
        except HTTPNotFound:
            return get_file_info(path, FileType.NotFound)  # this doesn't exist!
        return get_file_info(path, FileType.File, response.result.size_bytes, response.result.mtime)

    def _list_entries(self, path: str, delimiter: str = '/', max_amount: int = None):
        after = ''
        yielded = 0
        while True:
            response = self._client.objects.listObjects(
                repository=self.repository,
                ref=self.ref,
                prefix=path,
                after=after,
                delimiter=delimiter,
                amount=LISTING_PREFETCH_SIZE).response().result
            for result in response.get('results'):
                if result.path_type == 'object':
                    yield get_file_info(
                        result.path,
                        FileType.File,
                        result.size_bytes,
                        result.mtime,
                    )
                else:
                    yield get_file_info(
                        result.path,
                        FileType.Directory,
                    )
                yielded += 1
                if max_amount is not None and yielded >= max_amount:
                    return
            if not response.get('pagination').has_more:
                return  # no more things.
            after = response.get('pagination').next_offset
