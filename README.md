# [dramaX](https://github.com/KhaosResearch/dramaX) 

![CI](https://github.com/KhaosResearch/dramaX/actions/workflows/ci.yml/badge.svg)
![Python >=3.7](https://img.shields.io/badge/python-%3E=3.7-blue.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)

`dramaX` is a distributed workflow runner for Python.

It uses [RabbitMQ](https://www.rabbitmq.com) as a message broker and [Dramatiq](https://dramatiq.io) as a task queue.

## Getting Started

### Prerequisites

You can set up a minimal development environment using `docker compose`:

```bash
docker compose up -d
```

### Install

If you are feeling lucky and want to install the _latest version_ from source, clone the repository and install the dependencies:

```bash
git clone git@github.com:KhaosResearch/dramaX.git
cd dramaX
python -m pip install -e .
```

or if you are using uv:

```bash
git clone git@github.com:KhaosResearch/dramaX.git
cd dramaX
uv sync
```

## Usage

Create a `.env.local` file similar to [`.env.example`](.env.example) and fill in the values. A full list of configuration variables can be found in [`dramax/settings.py`](src/dramax/settings.py).
Then, you can run the command line client tool:

```sh
dramax -h
```

### Deploy server (optional)

Server can be [deployed](https://fastapi.tiangolo.com/deployment/) with *uvicorn*, a lightning-fast ASGI server, using the command-line client tool:

```sh
dramax server
```

Online documentation will be available at [`/api/docs`](http://0.0.0.0:8001/api/docs?access_token=dev).

### Spawn workers

Workers execute tasks in the background. They can be spawned using the command-line client tool:

```sh
dramax worker --processes 1
```

For a full list of valid command line arguments that can be passed to `dramax worker`, checkout `dramatiq -h`

## License

Copyright 2023 Khaos Research, all rights reserved.
