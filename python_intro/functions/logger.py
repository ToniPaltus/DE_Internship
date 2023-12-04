import logging


def init_logger(name: str) -> logging.getLogger:
    """A function to initialize logger with format, handlers
    input: name - string
    output: logger with name"""

    logger = logging.getLogger(name)
    FORMAT = "%(asctime)s - %(name)s:%(lineno)s - %(levelname)s - %(message)s"
    logger.setLevel(logging.DEBUG)

    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter(FORMAT))
    sh.setLevel(logging.DEBUG)

    fh = logging.FileHandler(filename="logs/test.log")
    fh.setFormatter(logging.Formatter(FORMAT))
    fh.setLevel(logging.DEBUG)

    logger.addHandler(sh)
    logger.addHandler(fh)

    logger.debug("Logger was initialized")
