#!/usr/bin/env python3
import pyarrow.orc
import pyarrow.parquet
from flask import Flask, request, jsonify, send_file
import lakefs
from lakefs.path import Path
from settings import LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY, LAKEFS_SERVER_ADDRESS

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    return send_file('README.md')


@app.route('/webhooks/format', methods=['POST'])
def webhook_formats():
    """
    A (very) simple webhook that validates all merged files are of a certain format
    Example lakeFS hook URL:
        http://<host:port>/webhooks/format?allow=parquet&allow=delta_lake&prefix=production/tables/
    """
    format_validators = {
        'delta_lake': lakefs.formats.is_delta_lake,
    }

    # Set-up a lakeFS client
    client = lakefs.Client(LAKEFS_SERVER_ADDRESS, LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY)

    # Read pre-merge hook details
    event = request.get_json()
    repo = event.get('repository_id')
    target_branch = event.get('branch_id')
    from_ref = event.get('source_ref')

    prefix = request.args.get('prefix')
    allowed_formats = request.args.getlist('allow')
    validation_funcs = [format_validators.get(f, lakefs.formats.has_extension(f)) for f in allowed_formats]

    errors = []
    for change in client.diff(repo, from_ref, target_branch, prefix=prefix):
        # we only care about new and overwritten files
        if change.type != 'added':
            continue

        if lakefs.formats.is_hadoop_hidden(Path(change.path)):
            continue  # let's skip hidden files

        p = Path(change.path)
        validation = map(lambda f: f(p), validation_funcs)
        if not any(validation):
            errors.append({'path': change.path, 'error': 'file format not allowed'})

    return jsonify({'errors': errors}), 200 if not errors else 400


@app.route('/webhooks/schema', methods=['POST'])
def webhook_schema():
    """
    A simple schema validation webhook to disallow certain field names under a given path
    Example lakeFS hook URL:
        http://<host:port>/webhooks/schema?disallow=user_&disallow=private_&prefix=public/
    """
    # Set-up a lakeFS client
    client = lakefs.Client(LAKEFS_SERVER_ADDRESS, LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY)

    # Read pre-merge hook details
    event = request.get_json()
    repo = event.get('repository_id')
    target_branch = event.get('branch_id')
    from_ref = event.get('source_ref')

    prefix = request.args.get('prefix')
    disallowed_prefixes = request.args.getlist('disallow')

    # Setup a PyArrow FileSystem that we can use to query data in the source ref
    fs = lakefs.get_filesystem(client, repo, from_ref)

    errors = []
    for change in client.diff(repo, from_ref, target_branch, prefix=prefix):
        # we only care about new and overwritten files
        if change.type not in ('added', 'changed'):
            continue

        path = Path(change.path)
        if path.extension == 'parquet':
            schema = pyarrow.parquet.read_schema(fs.open_input_file(change.path))
        # Do the same for ORC files
        elif path.extension == 'orc':
            orc_file = pyarrow.orc.ORCFile(fs.open_input_file(change.path))
            schema = orc_file.schema
        else:
            continue  # File format is not supported.

        # read schema and ensure we don't expose any user fields
        for column in schema:
            if any([column.name.startswith(prefix) for prefix in disallowed_prefixes]):
                errors.append({'path': change.path, 'error': f'column name not allowed: {column.name}'})

    return jsonify({'errors': errors}), 200 if not errors else 400


@app.route('/webhooks/dirty_check', methods=['POST'])
def webhook_dirty_check():
    """
    This webhook validates that merged change only creates a new directory, or replaces all objects within it.
    This is useful for immutable tables (or partitions) that are only ever calculated in their fullest, so any situation
        where data files are partially added or replaced, is treated as an error
    Example lakeFS hook URL:
        http://<host:port>/webhooks/dirty_check?prefix=hive/tables/
    """
    # Set-up a lakeFS client
    client = lakefs.Client(LAKEFS_SERVER_ADDRESS, LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY)

    # Read pre-merge hook details
    event = request.get_json()
    repo = event.get('repository_id')
    target_branch = event.get('branch_id')
    from_ref = event.get('source_ref')

    prefix = request.args.get('prefix')

    modified_dirs = []
    for change in client.diff_branch(repo, target_branch, prefix=prefix):
        dir_name = Path(change.path).dir_name + '/'
        if change.type not in ('added', 'changed'):
            continue  # We only care about directories that had files changed or added
        if not modified_dirs or modified_dirs[-1] != dir_name:
            modified_dirs.append(dir_name)

    # now we have an ordered list of directories that were modified under prefix
    errors = []
    for dir_name in modified_dirs:
        before = client.list(repo, client.get_last_commit(repo, from_ref), path=dir_name)
        after = client.list(repo, from_ref, path=dir_name)
        previous_data_files = set([obj.path for obj in before if obj.path_type == 'object' and obj.size_bytes > 0])
        current_data_files = [obj for obj in after if obj.path_type == 'object' and obj.size_bytes > 0]
        dirty_files = [o for o in current_data_files if o.path in previous_data_files]
        if len(dirty_files) == len(current_data_files):
            continue  # if all current files are "dirty", there wasn't a modification at all.
        for dirty_file in dirty_files:
            errors.append({'path': dirty_file.path, 'error': 'object is dirty'})

    return jsonify({'errors': errors}), 200 if not errors else 400


@app.route('/webhooks/commit_metadata', methods=['POST'])
def webhook_commit_metadata():
    """
    This is a pre-commit webhook that ensures commits that write to a given path also contain
        a certain set of metadata fields.
    Example lakeFS hook URL:
        http://<host:port>/webhooks/commit_metadata?prefix=data/daily/&fields=job_id&fields=owning_team
    """
    # Set-up a lakeFS client
    client = lakefs.Client(LAKEFS_SERVER_ADDRESS, LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY)

    # Read pre-merge hook details
    event = request.get_json()
    repo = event.get('repository_id')
    from_ref = event.get('source_ref')
    commit_metadata_fields = event.get('commit_metadata', {})

    # read request params
    prefix = request.args.get('prefix', '')
    fields = request.args.getlist('fields')

    errors = []
    has_changes_in_prefix = bool(list(client.diff_branch(repo, from_ref, prefix=prefix, max_amount=1)))
    if not has_changes_in_prefix:
        return jsonify({'errors': errors}), 200

    for field in fields:
        if field not in commit_metadata_fields:
            errors.append({'path': prefix, 'error': f'missing commit metadata field: {field}'})
            continue
        if not commit_metadata_fields.get(field):
            errors.append({'path': prefix, 'error': f'commit metadata field is empty: {field}'})

    return jsonify({'errors': errors}), 200 if not errors else 400
