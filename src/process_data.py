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
from constants import FLUX_CALIBRATOR_SOURCES, FLUXCAL_DIR, FLUXCAL_CLEANED_DIR, PULSAR_TYPES, SCRATCH_DIR
from cal_utils import CalUtils
from processor import Processor
from app_utils import AppUtils
from itertools import groupby
from slurm import Slurm
import time
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
	logger = Logger.getInstance(args)
	config = ConfigurationReader(args.config).get_config()
	db_manager = DBManager.get_instance(config.db_file)
	session = db_manager.get_session()


	query = session.query(Observation)
	query = query.filter(Observation.obs_type.in_(PULSAR_TYPES))	
	query = AppUtils.add_shortlist_filters(query, args)
	#query.filter(Observation.cleaned_file.isnot(None))

	observations = query.all()

	slurm = Slurm(config)


	observations = [x for x in observations if x.recleaned_file is None]
	print(observations)
	print(args)

	if args.slurm: 
		obs_groups = [list(g[1]) for g in groupby(observations, lambda o: o.obs_start_utc)]

		for group in obs_groups:

			biggest_size=0
			for o in group:			
				biggest_size = o.file_size if o.file_size > biggest_size else biggest_size
				

			memory = "{}g".format(round(biggest_size * 12))
			ncpus=1
			if biggest_size < 5:
				wall_time="2:00:00"
			elif biggest_size < 10:
				wall_time="4:00:00"
			else:
				wall_time="24:00:00"

			command = "python {} --config={} --obs_utcs=\"{}\" --stream_log_level=DEBUG".format(Path(__file__).resolve().as_posix(),args.config, group[0].obs_start_utc)
			job_id = slurm.run(group[0].source, group[0].obs_start_utc, command, memory, ncpus, wall_time)
			time.sleep(2)

			slurm_job = SlurmJob(id=job_id, state="queued")
			session.add(slurm_job)
			for o in group:
				o.slurm_job = slurm_job
				session.add(o)
			session.commit()

		logger.info("All jobs submitted..")


	else:

		for observation in observations:

			logger.debug("considering {}".format(observation))

			processor = Processor(config, observation)

			processor.preprocess()

			processor.clean()

			processor.calibrate()

			processor.reclean()

			processor.decimate()

			#processor.time()
 
if __name__ == "__main__":
	main()

