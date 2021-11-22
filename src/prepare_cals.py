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
from itertools import groupby



def get_args():
	argparser = argparse.ArgumentParser(description="prepare calibration files", 
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	argparser.add_argument("--config", dest="config", help="config file", required=True)
	AppUtils.add_shortlist_options(argparser)
	Logger.add_logger_argparse_options(argparser)

	args = argparser.parse_args()
	return args 

def group_by_freq(observations):
	freq_obs = {}
	for obs in observations: # obs object containing several chunks
		freq_chunk_group = [list(g[1]) for g in groupby(
				obs.observation_chunks, lambda o: o.cfreq)] # group chunks by frequency
		for freq_chunks in freq_chunk_group:
			in_dict_list = freq_obs.get(freq_chunks[0].cfreq, [])
			in_dict_list.append(freq_chunks)
			freq_obs[freq_chunks[0].cfreq] = in_dict_list
	return freq_obs

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

	fluxcal_observations = []
	polncal_observations = []
	
	timestamp = get_current_timestamp_String() 

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

		# if observation is a fluxcal/polncal, add it to the list of fluxcals/polncals
		if observation.is_flux_cal():
			fluxcal_observations.append(observation)
		elif observation.is_poln_cal():
			polncal_observations.append(observation)
		else:
			raise IncorrectFileHeaderException("Unknown observation type {} in header of {}".format(observation.obs_type, observation.obs_source_utc))

	# if there are fluxcals, add them to global list
	if len(fluxcal_observations) > 0:	
		freq_obs = group_by_freq(fluxcal_observations)

		for cfreq, v in freq_obs.items():
			logger.info("preparing flux calibrator for {} MHz".format(cfreq))
			cleaned_files = []
			for freq_chunks in v:
				for chunk in freq_chunks:
					cleaned_files.append(chunk.cleaned_file)
			fluxcal_list = "{}_{}_fluxcal_list.txt".format(timestamp, cfreq)
			local_db = cal_utils.create_cal_db(cleaned_files, fluxcal_list)
			cal_utils.add_to_fluxcal_db(local_db, cfreq)
	else:
		logger.warning("No flux calibrator observations found")

	# if there are polncals, add them to global list
	if len(polncal_observations) > 0:
		freq_obs = group_by_freq(polncal_observations)

		for cfreq, v in freq_obs.items():
			logger.info("preparing polarisation calibrator for {} MHz".format(cfreq))
			cleaned_files = []
			for freq_chunks in v:
				for chunk in freq_chunks:
					cleaned_files.append(chunk.cleaned_file)
			polncal_list = "{}_{}_polncal_list.txt".format(timestamp, cfreq)
			local_db = cal_utils.create_cal_db(cleaned_files, polncal_list)
			cal_utils.add_to_polncal_db(local_db)	
	else:
		logger.warning("No polarisation calibrator observations found")		

	#mark observation as processed
	for observation in observations:
		observation.processed = True
		db_manager.add_to_db(observation)

 
if __name__ == "__main__":
	main()