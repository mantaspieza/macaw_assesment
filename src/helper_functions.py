import logging


def logger_setup(
    log_name: str, logging_level=logging.INFO, file_handler_level=logging.ERROR
):

    logger = logging.getLogger(__name__)
    logger.setLevel(logging_level)

    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")

    file_handler = logging.FileHandler(log_name)
    file_handler.setLevel(file_handler_level)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


logger = logger_setup("test.log")
