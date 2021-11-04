import glob, argparse, datetime, sys
import numpy as np
from pathlib import Path, PurePath
import psrchive as ps
from astropy.time import Time
from log import Logger
import numpy.lib.recfunctions as rfn
from config_parser import ConfigurationReader
from db_orms import DBManager, Collection, Observation
from sqlalchemy import select
from gen_utils import split_and_strip,run_process,get_current_timestamp_String
import clfd
from clfd.interfaces import PsrchiveInterface
from rfi_utils import *
from constants import FLUX_CALIBRATOR_SOURCES, FLUXCAL_DIR, FLUXCAL_CLEANED_DIR, CALIBRATOR_TYPES,SCRATCH_DIR
from cal_utils import CalUtils
from processor import Processor
from app_utils import AppUtils



def get_args():
	argparser = argparse.ArgumentParser(description="prepare calibration files", 
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	argparser.add_argument("--config", dest="config", help="config file", required=True)
	AppUtils.add_shortlist_options(argparser)
	Logger.add_logger_argparse_options(argparser)

	args = argparser.parse_args()
	return args

def main():

	# get arguments, and with that initialise the logger, and obtain the config file and the DB session
	args = get_args()
	logger = Logger.getInstance(args)
	config = ConfigurationReader(args.config).get_config()
	db_manager = DBManager.get_instance(config.db_file)
	session = db_manager.get_session()
	cal_utils = CalUtils(config)


	query = session.query(Observation)
	query = query.filter(Observation.obs_type.in_(CALIBRATOR_TYPES))
	query = AppUtils.add_shortlist_filters(query, args)
	


	observations = query.all()


	fluxcal_archives = []
	polncal_archives = []

	for observation in observations:

		logger.debug("considering {}".format(observation))

		cal_utils.fix_cal_type(observation)

		processor = Processor(config, observation)

		processor.clean()


	fluxcal_archives = [x.cleaned_file for x in observations if x.is_flux_cal()]
	polncal_archives = [x.cleaned_file for x in observations if x.is_poln_cal()]

	timestamp = get_current_timestamp_String() 

	fluxcal_list = timestamp + "_fluxcal_list.txt"
	polncal_list = timestamp + "_polncal_list.txt"

	local_db = cal_utils.create_cal_db(fluxcal_archives,fluxcal_list)
	cal_utils.add_to_fluxcal_db(local_db)


	local_db = cal_utils.create_cal_db(polncal_archives,polncal_list)
	cal_utils.add_to_polncal_db(local_db)


 
if __name__ == "__main__":
	main()