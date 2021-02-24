from collections import namedtuple
from typing import Iterator
from urllib.parse import urlparse

from bravado.requests_client import RequestsClient
from bravado.client import SwaggerClient
from pyarrow.fs import PyFileSystem

from .fs import pyarrow_fs

# Determines how many objects to fetch on each diff/list iteration from the lakeFS API.
PREFETCH_CURSOR_SIZE = 1000


class Client(object):
    """
    Client is a lakeFS OpenAPI client, generated dynamically using Bravado.
    To instantiate a new client, it must have access to a running lakeFS server.

    Example usage:
    >>> import lakefs
    >>> import pyarrow.parquet as pq
    >>> client = lakefs.Client('http://localhost:8000', '<lakeFS access key ID>', '<lakeFS secret key>')
    >>> # Explore a diff between two branches
    >>> # Get a PyArrow compatible, read-only filesystem on top of lakeFS
    >>> fs = get_filesystem(client, 'my-repo-name', 'experiment-branch')
    >>> for change in client.diff('my-repo-name', 'experiment-branch', 'main', prefix='collections/production/'):
    >>>     if change.type == 'added':
    >>>         schema = pq.read_schema(fs.open_input_file(change.path))
    >>>         for field in schema:
    >>>             pass  # Do something with the schema!
    """

    def __init__(self, base_url: str, access_key: str, secret_key: str):
        url = urlparse(base_url)
        self._http_client = RequestsClient()
        self._http_client.set_basic_auth(url.netloc, access_key, secret_key)
        self._client = SwaggerClient.from_url(
            f'{base_url}/swagger.json',
            http_client=self._http_client,
            config={"validate_swagger_spec": False})

    def diff_branch(self, repository: str, branch: str, prefix: str = '',
                    prefetch_amount: int = PREFETCH_CURSOR_SIZE,
                    max_amount: int = None) -> Iterator[namedtuple]:
        after = prefix
        amount = 0
        if max_amount is not None:
            prefetch_amount = min(prefetch_amount, max_amount)
        while True:
            response = self._client.branches.diffBranch(
                repository=repository,
                branch=branch,
                after=after,
                amount=prefetch_amount).response().result
            for change in response.get('results'):
                if not change.path.startswith(prefix):
                    return  # we're done since path > prefix
                yield change
                amount += 1
                if max_amount is not None and amount >= max_amount:
                    return
            if not response.get('pagination').has_more:
                return  # no more things.
            after = response.get('pagination').next_offset

    def diff(self, repository: str, from_ref: str, to_ref: str, prefix: str = '',
             prefetch_amount: int = PREFETCH_CURSOR_SIZE) -> Iterator[namedtuple]:
        after = prefix
        while True:
            response = self._client.refs.diffRefs(
                repository=repository,
                leftRef=from_ref,
                rightRef=to_ref,
                after=after,
                amount=prefetch_amount).response().result
            for change in response.get('results'):
                if not change.path.startswith(prefix):
                    return  # we're done since path > prefix
                yield change
            if not response.get('pagination').has_more:
                return  # no more things.
            after = response.get('pagination').next_offset

    def list(self, repository: str, ref: str, path: str, delimiter: str = '/', max_amount: int = None):
        if not max_amount or max_amount == 0:
            return
        after = ''
        amount = 0
        while True:
            response = self._client.objects.listObjects(
                repository=repository,
                ref=ref,
                prefix=path,
                after=after,
                delimiter=delimiter,
                amount=PREFETCH_CURSOR_SIZE).response().result
            for result in response.get('results'):
                yield result
                amount += 1
                if max_amount and amount >= max_amount:
                    return
            if not response.get('pagination').has_more:
                return  # no more things.
            after = response.get('pagination').next_offset

    def get_object(self, repository: str, ref: str, path: str):
        response = self._client.objects.getObject(
            repository=repository,
            ref=ref,
            path=path).response()
        return response.result

    def stat_object(self, repository: str, ref: str, path: str):
        response = self._client.objects.statObject(
            repository=repository,
            ref=ref,
            path=path).response()
        return response.result


def get_filesystem(client: Client, repository: str, ref: str) -> PyFileSystem:
    return pyarrow_fs(client=client, repository=repository, ref=ref)
