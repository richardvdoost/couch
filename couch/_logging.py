import logging


def get_logger(name: str, level: int = logging.WARNING) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if name == "__main__" and not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(ColorFormatter())
        logger.addHandler(ch)

    return logger


class ColorFormatter(logging.Formatter):
    grey = "\x1b[90;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    reset = "\x1b[0m"
    basic_format = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    error_format = " (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + basic_format + reset,
        logging.INFO: basic_format,
        logging.WARNING: yellow + basic_format + reset,
        logging.ERROR: red + basic_format + reset + error_format,
        logging.CRITICAL: red + basic_format + reset + error_format,
    }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, "%H:%M:%S")

        return formatter.format(record)
