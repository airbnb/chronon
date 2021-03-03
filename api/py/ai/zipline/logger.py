import logging

LOG_FORMAT = '[%(asctime)-11s] %(levelname)s [%(filename)s:%(lineno)d] %(message)s'


def get_logger(log_level=logging.INFO):
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    return logger
