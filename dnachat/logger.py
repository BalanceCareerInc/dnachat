# -*-coding:utf8-*-
import logging

logger = logging.getLogger(__name__)


def init_logger(filename, level):
    logger.setLevel(level)

    handler = logging.FileHandler(filename)
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s:%(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


