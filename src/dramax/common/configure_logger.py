import logging

import structlog


def configure_logger() -> None:
    logging.getLogger("dramax.request").setLevel(logging.WARNING)

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",  # Evita que duplique output con structlog
    )

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="%H:%M:%S"),  # Hora compacta
            structlog.stdlib.add_log_level,  # Añade nivel (INFO, ERR, etc.)
            structlog.stdlib.add_logger_name,  # Añade 'logger name'
            structlog.dev.ConsoleRenderer(colors=True),  # Salida con colores y legible
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.INFO
        ),  # Define nivel desde aquí
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
