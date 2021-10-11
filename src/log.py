import atexit
import logging
import logging.handlers
import os

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"
COLORS = {
    'WARNING': YELLOW,
    'INFO': BLUE,
    'DEBUG': WHITE,
    'CRITICAL': CYAN,
    'ERROR': RED
}



class ColoredFormatter(logging.Formatter):
    def __init__(self, msg):
        logging.Formatter.__init__(self, msg)

    def format(self, record):
        levelname = record.levelname
        if levelname in COLORS:
            record.levelname = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
        return logging.Formatter.format(self, record)

formatter = ColoredFormatter('%(asctime)s: %(name)s: %(levelname)s: %(module)s: %(funcName)s: %(threadName)s: %(message)s')


class Logger(object):

    __instance = None
    @staticmethod
    def getInstance():
        """ Static access method. """
        if Logger.__instance == None:
            Logger()
        return Logger.__instance

    def __init__(self, path="", file_name="uwl.log", file_level=None):
        """ Virtually private constructor. """
        if Logger.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            Logger.__instance = self
        logger = logging.getLogger(file_name)
        logger.setLevel(logging.INFO if file_level is None else file_level)
        logger.propagate = False

        file_handler = logging.FileHandler(file_name)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO if file_level is None else file_level)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        if os.path.exists(os.path.join(path, file_name)):
            with open(os.path.join(path, file_name), 'a') as f:
                f.write(30 * "#")
                f.write("\n")
        logger.info("File handler created")

        self._logger = logger

    @property
    def logger(self):
        return self._logger
    







def init_logging(path="", file_name="uwl.log", file_level=None):

    logger = logging.getLogger(file_name)
    logger.setLevel(logging.INFO if file_level is None else file_level)
    logger.propagate = False

    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO if file_level is None else file_level)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    if os.path.exists(os.path.join(path, file_name)):
        with open(os.path.join(path, file_name), 'a') as f:
            f.write(30 * "#")
            f.write("\n")
    logger.info("File handler created")

    return logger