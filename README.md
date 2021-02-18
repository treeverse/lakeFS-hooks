# lakeFS-Flask-Webhook

This repository provides a simple [lakeFS](https://www.lakefs.io/) webhook for pre-commit and pre-merge validation of data objects.

Using lakeFS hooks, data engnieers can create an object store based data lake that provides a certain set of codified rules. Some examples:

1. under `production/collections/`, only Parquet and ORC formats are allowed.
1. Every new collection under `collections/` must also be registerd in Hive Meta Store (or another metadata registry)
1. No breaking schema changes are allowed under `collections/important/`
1. changes to the `events/` collection must pass a set of data validation rules
1. New partitons of `huge_table/` must contain > 500,000 records to be considered valid. Additionally, the `clicks` column must sum to -+50% of the previous day's value. 

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

1. A very terse lakeFS Python client that provides a diff generator. When triggered by a pre-merge hook, it is very useful to be able to see what changes are going to be applied. See `lakefs/client.py`
1. A simple, naive, read-only PyArrow FileSystem implementation for reading data objects from lakeFS. This allows using PyArrow to read Parquet, ORC and other formats using PyArrow - to inspect their metadata or to construct queryable tables for testing and validation.
1. An implementation of a Flask server that accepts a pre-merge event, parses it, requests a diff from the lakeFS client, and applies some validation logic using PyArrow. This is the part you'll probably want to modify to your needs.
1. A Dockerfile to containerize a webhook for deployment, that can run a Flask server.

## Installation

To get started, clone this repo locally, as you will probably want to modify it to your needs:

```sh
$ git clone git@github.com:treeverse/lakeFS-Flask-Webhook.git
$ cd lakeFS-Flask-Webhook/
# edit server.py
```

## Building a Docker image

To build a docker image, run the following command:

```sh
$ docker build -t lakefs-hook:latest .
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
  }' 'http://localhost:5000/'
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
    lakefs-hook:latest
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

