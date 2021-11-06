import argparse
import datetime
import glob
import sys
import time
from itertools import groupby
from pathlib import Path, PurePath

import clfd
import numpy as np
import numpy.lib.recfunctions as rfn
import psrchive as ps
from astropy.time import Time
from clfd.interfaces import PsrchiveInterface
from sqlalchemy import select

from app_utils import AppUtils
from cal_utils import CalUtils
from config_parser import ConfigurationReader
from constants import (FLUX_CALIBRATOR_SOURCES, FLUXCAL_CLEANED_DIR,
					   FLUXCAL_DIR, PULSAR_TYPES, SCRATCH_DIR)
from db_orms import Collection, DBManager, ObservationChunk
from gen_utils import (get_current_timestamp_String, run_process,
					   split_and_strip)
from log import Logger
from processor import Processor
from rfi_utils import *
from session import ObservingSession
from slurm import Slurm


def get_args():
	argparser = argparse.ArgumentParser(description="process pulsar data", 
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	argparser.add_argument("--config", dest="config", help="config file", required=True)

	argparser.add_argument("--with-slurm", dest="slurm", help="Queue with slurm", action='store_true')

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


	# get the list of observations to process
	query = db_manager.get_session().query(Observation)
	query = query.filter(Observation.obs_type.in_(PULSAR_TYPES))
	query = query.filter(Observation.processed == False)
	query = AppUtils.add_shortlist_filters(query, args)

	observations = query.all()	



	slurm = Slurm(config)


	if args.slurm: 
		
		for observation in observations:

			biggest_size=0
			for o in observation.observation_chunks:			
				biggest_size = o.file_size if o.file_size > biggest_size else biggest_size
				

			memory = "{}g".format(round(biggest_size * 12))
			ncpus=1
			if biggest_size < 5:
				wall_time="2:00:00"
			elif biggest_size < 10:
				wall_time="4:00:00"
			else:
				wall_time="24:00:00"

			command = "python {} --config={} --obs_utcs=\"{}\" --stream_log_level=DEBUG".format(Path(__file__).resolve().as_posix(),
																								Path(args.config).resolve().as_posix(), 
							observation.obs_start_utc)
			job_id = slurm.launch(
				observation.source, observation.obs_start_utc, command, memory, ncpus, wall_time)
			time.sleep(2)

			slurm_job = SlurmJob(id=job_id, state="QUEUED")
			db_manager.get_session().add(slurm_job)
			observation.slurm_job = slurm_job
			db_manager.get_session().add(observation)
			db_manager.get_session().commit()

		logger.info("All jobs submitted..")


	else:

		for observation in observations:
			observing_session = ObservingSession(config, observation)
			observing_session.process() 
			observation.processed = True
			db_manager.add_to_db(observation)

 
if __name__ == "__main__":
	main()

