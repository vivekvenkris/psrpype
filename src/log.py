import atexit
import datetime
import logging
import logging.handlers
import os
from pathlib import Path

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
	def get_instance(args = None):
		""" Static access method. """
		if Logger.__instance is None:
			Logger.__instance = Logger(args)
		return Logger.__instance.logger

	def share_handlers_with(self, logger):
		for handler in self.logger.handlers:
			logger.addHandler(handler)

	def __init__(self, args):
		""" Virtually private constructor. """
		if Logger.__instance != None:
			raise Exception("This class is a singleton!")
		else:
			Logger.__instance = self
		logger = logging.getLogger("logger") 
		logger.setLevel('DEBUG')
		logger.propagate = True


		if args.log_to_file:

			print("started logging to file", args.file_log_level, args.stream_log_level, logging.getLevelName(args.file_log_level), logging.getLevelName(args.stream_log_level))

			file_name = args.log_file
			if args.log_file is None or args.log_file == "timestamp":
				file_name = "{}.log".format(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S").replace("T","-"))

			file_handler = logging.FileHandler(file_name)
			file_handler.setFormatter(formatter)
			file_handler.setLevel(logging.getLevelName(args.file_log_level))
			logger.addHandler(file_handler)

			if os.path.exists(file_name):
				with open(os.path.join(file_name), 'a') as f:
					f.write(30 * "#")
					f.write("\n")
			logger.info("File handler created")

		if args.log_to_stream:

			stream_handler = logging.StreamHandler()
			stream_handler.setLevel(logging.getLevelName(args.stream_log_level))
			stream_handler.setFormatter(formatter)

			logger.addHandler(stream_handler)



		self._logger = logger

	@property
	def logger(self):
		return self._logger

	@staticmethod
	def add_logger_argparse_options(argparser):

		log_group = argparser.add_argument_group('Logging options')
		log_group.add_argument("-l", "--log_to_stream", dest="log_to_stream", help="log_to output stream (print on screen)?", action="store_true", default=True)
		log_group.add_argument("-x", "--log_to_file", dest="log_to_file",  help="Also log to file?", action="store_true", default=False)
		log_group.add_argument("--stream_log_level", dest="stream_log_level", help="Log level for output stream (print on screen): one of DEBUG, INFO, WARN, FATAL, all in caps.", default="WARN")
		log_group.add_argument("--file_log_level", dest="file_log_level", help="Log level for log file: one of DEBUG, INFO, WARN, FATAL, all in caps.", default="WARN")
		log_group.add_argument("--log_file", dest="log_file", help="Filename for logging", default="timestamp")
		
