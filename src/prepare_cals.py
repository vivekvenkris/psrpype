import glob, argparse, datetime, sys
import numpy as np
from pathlib import Path, PurePath
import psrchive as ps
from astropy.time import Time
from log import Logger
import numpy.lib.recfunctions as rfn
from config_parser import ConfigurationReader
from db_orms import DBManager, Collection, ObservationChunk
from sqlalchemy import select
from gen_utils import split_and_strip,run_process,get_current_timestamp_String
import clfd
from clfd.interfaces import PsrchiveInterface
from rfi_utils import *
from constants import FLUX_CALIBRATOR_SOURCES, FLUXCAL_DIR, FLUXCAL_CLEANED_DIR, CALIBRATOR_TYPES,SCRATCH_DIR
from cal_utils import CalUtils
from processor import Processor
from app_utils import AppUtils
from exceptions import IncorrectFileHeaderException



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
	logger = Logger.get_instance(args)
	config = ConfigurationReader(args.config).get_config()
	db_manager = DBManager.get_instance(config.db_file)
	cal_utils = CalUtils(config)

	# get the list of observations to process
	query = db_manager.get_session().query(Observation)
	query = query.filter(Observation.obs_type.in_(CALIBRATOR_TYPES))
	query = query.filter(Observation.processed == False)
	query = AppUtils.add_shortlist_filters(query, args)

	observations = query.all()

	fluxcal_archives = []
	polncal_archives = []
	
	timestamp = get_current_timestamp_String() 

	fluxcal_archives = []
	polncal_archives = []
	

	# process each observation
	for observation in observations:

		# process each chunk if it is not already processed
		for observation_chunk in [o for o in observation.observation_chunks if o.processed == False]:

			logger.debug("considering {}".format(observation_chunk))

			processor = Processor(config, observation_chunk)
			processor.preprocess()
			processor.clean()
			observation_chunk.processed = True
		
			db_manager.add_to_db(observation_chunk)


		cleaned_files = [x.cleaned_file for x in observation.observation_chunks]
		if observation.is_flux_cal():
			fluxcal_archives.append(cleaned_files)
		elif observation.is_poln_cal():
			polncal_archives.append(cleaned_files)
		else:
			raise IncorrectFileHeaderException("Unknown observation type {} in header of {}".format(observation.obs_type, observation.obs_source_utc))
	

	if len(fluxcal_archives) > 0:
		logger.info("preparing flux calibrator files")
		fluxcal_list = timestamp + "_fluxcal_list.txt"
		local_db = cal_utils.create_cal_db(fluxcal_archives, fluxcal_list)
		cal_utils.add_to_fluxcal_db(local_db)

	if len(polncal_archives) > 0:
		logger.info("preparing poln calibrator files")
		polncal_list = timestamp + "_polncal_list.txt"
		local_db = cal_utils.create_cal_db(polncal_archives, polncal_list)
		cal_utils.add_to_polncal_db(local_db)

	#mark observation as processed
	for observation in observations:
		observation.processed = True
		db_manager.add_to_db(observation)

 
if __name__ == "__main__":
	main()