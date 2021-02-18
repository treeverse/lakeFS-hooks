#!/usr/bin/env python3
import pathlib

import lakefs
from settings import LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY, LAKEFS_SERVER_ADDRESS

import pyarrow.orc
import pyarrow.parquet

from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/', methods=['POST'])
def run_hook():
    """
    An example lakeFS pre-merge hook that does some naive validation.

    It accepts a lakeFS hook event, asks for the list of modifications that the merge will trigger,
        and ensures they all meet certain criteria based on their format and schema.
    """

    # Set-up a lakeFS client
    client = lakefs.Client(LAKEFS_SERVER_ADDRESS, LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY)

    # Read pre-merge hook details
    event = request.get_json()
    repo = event.get('repository_id')
    target_branch = event.get('branch_id')
    from_ref = event.get('source_ref')

    # Get the list of paths that are going to be modified
    # We only care about stuff that happens under the 'collections/' prefix.
    diff = client.diff(repo, from_ref, target_branch, prefix='collections/')

    # Setup a PyArrow FileSystem that we can use to query data in the source ref
    fs = client.filesystem(repo, from_ref)

    # Run actual validation logic
    errs = []
    for change in diff:
        # we only care about new and overwritten files
        if change.type not in ('added', 'changed'):
            continue

        # Parse path
        path = pathlib.PurePosixPath(change.path)

        # Delta Lake log files are allowed and skipped
        if path.parent is not None \
                and path.parent.name == '_delta_log' \
                and path.suffix in {'.json', '.parquet'}:
            continue  # We allow json files, but only for Delta logs

        # ensure remaining files are of an allowed format
        allowed_file_formats = {'.parquet', '.orc'}
        if path.suffix not in allowed_file_formats:
            errs.append({'path': change.path, 'error': 'file format not allowed'})
            continue

        # use PyArrow to read file schema (either from Parquet or ORC)
        schema = None
        if path.suffix == '.parquet':
            schema = pyarrow.parquet.read_schema(fs.open_input_file(change.path))
        # Do the same for ORC files
        elif path.suffix == '.orc':
            orc_file = pyarrow.orc.ORCFile(fs.open_input_file(change.path))
            schema = orc_file.schema

        # read schema and ensure we don't expose any user fields
        schema_errors = False
        for column in schema:
            if column.name.startswith('user_'):
                errs.append({'path': change.path, 'error': f'column name not allowed: {column.name}'})
                schema_errors = True
                break
        if schema_errors:
            continue

    # Return a report back to lakeFS
    code = 200
    if len(errs) > 0:
        code = 400  # Block this merge!

    return jsonify({'validation_errors': errs}), code
