FROM ghcr.io/astral-sh/uv:python3.8-bookworm-slim

RUN apt-get update && apt-get install -y git psmisc

WORKDIR /dramax

RUN groupadd --system --gid 999 agoradev \
  && useradd  --system --gid 999 --uid 999 --create-home agoradev

RUN chown agoradev:agoradev /dramax
RUN mkdir -p /dramax/logs && chown agoradev:agoradev /dramax/logs

USER agoradev

COPY . .

USER root

RUN chmod +x ./scripts/start-all.sh

USER agoradev

EXPOSE 8005

RUN uv sync

ENV PATH="/dramax/.venv/bin/:${PATH}"

ENTRYPOINT ["./scripts/start-all.sh"]