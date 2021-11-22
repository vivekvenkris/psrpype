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
from slurm import SlurmLauncher, SlurmChecker


def get_args():
	argparser = argparse.ArgumentParser(description="process pulsar data", 
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	argparser.add_argument("--config", dest="config", help="config file", required=True)

	argparser.add_argument("--with-slurm", dest="slurm", help="Queue with slurm", action='store_true')
	argparser.add_argument("--consolidate", dest="consolidate", help="psradd recleaned files + produce decimated products", action='store_true')

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
	query = db_manager.get_session().query(Observation).join(SlurmJob, Observation.slurm_id == SlurmJob.id, isouter=True)
	query = query.filter(Observation.obs_type.in_(PULSAR_TYPES))

	# 1. processed is false, no slurm job attached
	# 2. processed is false, slurm job is not running
	# 3. processed is false, slurm job is failed
	query = query.filter( ((Observation.processed == False) & (Observation.slurm_job == None))| 
							((Observation.processed == False) & ((Observation.slurm_job != None) & (SlurmJob.state != 'RUNNING'))) |
							((Observation.slurm_job != None) & 	((SlurmJob.state == 'FAILED') | (SlurmJob.state == 'OUT_OF_MEMORY'))) 
						)

	query = AppUtils.add_shortlist_filters(query, args)

	observations = query.all()	

	if(len(observations) == 0):
		logger.info("No observations to process")
		return

	else:
		logger.info("Processing {} observations".format(len(observations)))
		for obs in observations:
			logger.debug("Processing {} {} {}".format(obs.source, obs.obs_start_utc, obs.cfreq))
		

	slurm_launcher = SlurmLauncher(config)


	if args.slurm: 
		job_ids = []
		for observation in observations:
			consolidate_flag= "--consolidate" if args.consolidate else ""
			command = "python {} --config={} --obs_utcs=\"{}\" -c \"{}\" {} --stream_log_level=DEBUG".format(Path(__file__).resolve().as_posix(),
																								Path(args.config).resolve().as_posix(), 
                            observation.obs_start_utc, observation.cfreq, consolidate_flag)
			job_id = slurm_launcher.launch(observation, command)
			job_ids.append(job_id)
			time.sleep(2)

			slurm_job = SlurmJob(id=job_id, state="QUEUED")
			observation.slurm_job = slurm_job
			db_manager.add_to_db([observation,slurm_job])

		logger.info("All jobs submitted..")

		slurm_checker = SlurmChecker(job_ids)
		SlurmChecker.signal_init()
		slurm_checker.start()
		slurm_checker.join()


	else:

		for observation in observations:
			observing_session = ObservingSession(config, observation)
			observing_session.process(consolidate=args.consolidate) 
			observation.processed = True
		
		db_manager.add_to_db(observations)



if __name__ == "__main__":
	main()

