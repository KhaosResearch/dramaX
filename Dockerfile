FROM ghcr.io/astral-sh/uv:python3.8-bookworm-slim

RUN apt-get update && apt-get install -y git psmisc

WORKDIR /dramax

RUN groupadd --system --gid 999 agoradev \
  && useradd  --system --gid 999 --uid 999 --create-home agoradev

RUN mkdir -p /dramax/logs

COPY . .

RUN chmod +x ./scripts/start-all.sh

EXPOSE 8005

RUN uv sync

ENV PATH="/dramax/.venv/bin/:${PATH}"

ENTRYPOINT ["./scripts/start-all.sh"]