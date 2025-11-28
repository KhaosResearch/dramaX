FROM ghcr.io/astral-sh/uv:python3.8-bookworm-slim

RUN apt-get update && apt-get install -y git

RUN groupadd --system --gid 999 agoradev \
  && useradd  --system --gid 999 --uid 999 --create-home agoradev

USER agoradev

WORKDIR /dramax

COPY . .

RUN uv sync

ENV PATH="/dramax/.venv/bin/:${PATH}"

CMD [ "bash", "-c", "dramax server" ]
