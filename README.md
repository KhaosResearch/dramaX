# [dramaX](https://github.com/KhaosResearch/dramaX)

![CI](https://github.com/KhaosResearch/dramaX/actions/workflows/ci.yml/badge.svg)
![Python ==3.10.2](https://img.shields.io/badge/python-%3E=3.10.2-blue.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)

`dramaX` is a distributed workflow runner for Python.  

It uses [RabbitMQ](https://www.rabbitmq.com) as a message broker and [Dramatiq](https://dramatiq.io) as a task queue.

## Considerations

* Install Python 3.10.2.

* Add this version of Python to the Dockerfile and call the Dockerfile from Docker Compose file.

* Download the 4.4.16 image from MongoDB, as the previous version required AVX support from the CPU (these are instructions that provide capabilities to improve application performance).

* In Docker Compose, change the value of the RABBITMQ_DEFAULT_USER and RABBITMQ_DEFAULT_PASS environment variables from 'rabbit' to 'root'.

## Getting Started

### Prerequisites

* Python 3.10.2

* Docker & Docker Compose

You can set up a minimal development environment using `docker compose`:

```bash
git clone git@github.com:KhaosResearch/dramaX.git
cd dramaX
docker compose up -d
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
