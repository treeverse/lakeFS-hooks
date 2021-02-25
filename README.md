# lakeFS-Flask-Webhook

This repository provides a set of simple [lakeFS](https://www.lakefs.io/) webhooks for pre-commit and pre-merge validation of data objects.

By setting these rules, a lakeFS-based data lake can ensure that production branches only ever contain valid, quality data - but still allows others to experiment with untested version on isolated branches.

This repository is meant to be forked and adapted to do actual useful validation (see the `run_hook` function in server.py for the actual validation logic)

## Table of Contents

- [What's included](#whats-included)
- [Installation](#installation)
- [Building a Docker image](#building-a-docker-image)
- [Running a Server Locally](#running-a-server-locally)
- [Usage](#usage)
- [Support](#support)
- [Community](#community)


## What's included

This project contains a few basic building blocks that should make building custom lakeFS pre-merge/pre-commit hooks easier:

1. A very terse lakeFS Python client that provides basic reading and diffing functions
1. A simple, naive, read-only PyArrow FileSystem implementation for reading data objects from lakeFS.
   This allows using PyArrow to read Parquet, ORC and other formats using PyArrow - to inspect their metadata or to construct queryable tables for testing and validation
1. A set of reusable webhooks that could be used for common CI requirements (see below)
1. A Dockerfile to containerize a webhook server for deployment

### Included Webhooks

#### File Format Validator

This webhook checks new files to ensure they are of a set of allowed data format. Could be scoped to a certain prefix.

Example usage as a pre-merge hook in lakeFS:

```yaml
---
name: ParquetOnlyInProduction
description: This webhook ensures that only parquet files are written under production/
on:
  pre-merge:
    branches:
      - master
hooks:
  - id: production_format_validator
    type: webhook
    description: Validate file formats
    properties:
      url: "http://<host:port>/webhooks/format"
      query_params:
        allow: ["parquet", "delta_lake"]
        prefix: production/
```

#### Basic File Schema Validator

This webhook Parquet and ORC files to ensure they don't contain a black list of column names (or name prefixes).
This is useful when we want to avoid accidental PII exposure.

Example usage as a pre-merge hook in lakeFS:

```yaml
---
name: NoUserColumnsUnderPub
description: >-
  This webhook ensures that files with columns 
  beginning with "user_" can't be written to public/ 
on:
  pre-merge:
    branches:
      - master
hooks:
  - id: pub_prevent_user_columns
    type: webhook
    description: Ensure no user_* columns under public/
    properties:
      url: "http://<host:port>/webhooks/schema"
      query_params:
        disallow: ["user_", "private_"]
        prefix: public/
```

#### Partition Dirty Checker

In certain cases, we want to ensure partitions (or directories) are completely immutable.
This means we allow writing to a directory only if:
   - we overwrite all the files in it
   - we add files but also delete all previous content

In this case, if files were added or replaced, but some previous content remains, we consider it "dirty" and fail the commit

Example usage as a pre-commit hook in lakeFS:

```yaml
---
name: NoDirtyPartitionsInProduction
description: Check all partitions remain immutable under tables/hive/
on:
  pre-commit:
    branches:
      - "*"
hooks:
  - id: hive_ensure_immutable
    type: webhook
    description: Check all hive partitions are either full written or fully replaced
    properties:
      url: "http://<host:port>/webhooks/dirty_check"
      query_params:
        prefix: tables/hive/
```

#### Commit Tag Validator

In production, we want to ensure commits carry enough metadata to be useful for lineage and traceability.

Example usage as a pre-commit hook in lakeFS:

```yaml
---
name: EnsureProductionCommitMetadata
description: >-
  Check commits that write to production/ that 
  they contain a set of mandatory metadata fields.
  These fields must not be empty.
on:
  pre-commit:
    branches:
      - "*"
hooks:
  - id: production_ensure_commit_metadata
    type: webhook
    description: Check all commits that write to production/ for mandatory metadata fields
    properties:
      url: "http://<host:port>/webhooks/commit_metadata"
      query_params:
        prefix: production/
        fields: [airflow_dag_run_url, job_git_commit, update_sla, sources]
```


## Installation

To get started, clone this repo locally, as you might also want to modify it to your needs:

```sh
$ git clone git@github.com:treeverse/lakeFS-Flask-Webhook.git
$ cd lakeFS-Flask-Webhook/
# edit server.py
```

## Building a Docker image

To build a docker image, run the following command:

```sh
$ docker build -t lakefs-hooks:latest .
# optionally, tag it and push it to a repository for deployment
```

## Running a server locally

```sh
# You should probably be using something like virtualenv/pipenv
$ pip install -r requirements.txt
$ export LAKEFS_SERVER_ADDRESS="http://lakefs.example.com"
$ export LAKEFS_ACCESS_KEY_ID="<access key ID of a lakeFS user>"
$ export LAKEFS_SECRET_ACCESS_KEY="<secret access key for the give key ID>"
$ flask run
```

You can now test it by passing an example pre-merge event using cURL:

```sh
curl -v -XPOST -H 'Content-Type: application/json' \
  -d'{
       "event_type": "pre-merge",
       "event_time": "2021-02-17T11:04:18Z",
       "action_name": "test action",
       "hook_id": "hook_id",
       "repository_id": "my-lakefs-repository",
       "branch_id": "main",
       "source_ref": "220158b4b316e536e024aaaaf76b2377a6c71dfd6b974ca3a49354a9bdd0dbc3",
       "commit_message": "a commit message",
       "committer": "user1"
  }' 'http://localhost:5000/webhooks/schema'
```


## Running the Docker Container

See [Building a Docker Image](#building-a-docker-image) above for build instructions.

To run the resulting image using the Docker command line:

```shell
$ docker run \
    -e LAKEFS_SERVER_ADDRESS='http://lakefs.example.com' \
    -e LAKEFS_ACCESS_KEY_ID='<access key ID of a lakeFS user>' \
    -e LAKEFS_SECRET_ACCESS_KEY='<secret access key for the give key ID>' \
    -p 5000:5000 \
    lakefs-hooks:latest
```

## Support

Please [open an issue](https://github.com/treeverse/lakeFS-Flask-Webhook/issues/new) for support or contributions.

For more information on [lakeFS](https://www.lakefs.io/), please see the [official lakeFS documentation](https://docs.lakefs.io/).

## Community

Stay up to date and get lakeFS support via:

- [Slack](https://join.slack.com/t/lakefs/shared_invite/zt-ks1fwp0w-bgD9PIekW86WF25nE_8_tw) (to get help from our team and other users).
- [Twitter](https://twitter.com/lakeFS) (follow for updates and news)
- [YouTube](https://www.youtube.com/channel/UCZiDUd28ex47BTLuehb1qSA) (learn from video tutorials)
- [Contact us](https://lakefs.io/contact-us/) (for anything)

