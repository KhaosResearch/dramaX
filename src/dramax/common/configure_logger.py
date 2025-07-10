import logging

import structlog


def configure_logger() -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        level=logging.INFO,
    )

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="%H:%M:%S"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),  # Usa logging.getLogger(name)
        cache_logger_on_first_use=True,
    )
