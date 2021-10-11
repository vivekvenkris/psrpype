from utils import *
from log import *
import logging


logger = init_logging(file_name='test.log')

run_subprocess("test","csh -c pwd",logger) 