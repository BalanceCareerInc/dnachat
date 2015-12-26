# -*-coding:utf8-*-
import logging
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger(__name__)


def init_logger(filename, level):
    logger.setLevel(level)

    handler = TimedRotatingFileHandler(filename, 'D', 1)
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s:%(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
