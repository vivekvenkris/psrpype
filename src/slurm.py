import argparse
import hashlib
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
import signal
import math
from config_parser import ConfigurationReader
from constants import PULSAR_TYPES
from db_orms import DBManager, Observation, SlurmJob
from gen_utils import run_process
from log import Logger

TEMPLATE = """\
#!/bin/bash

#SBATCH -e {log_dir}/{name}.%J.err
#SBATCH -o {log_dir}/{name}.%J.out
#SBATCH -J {name}

#SBATCH --mail-user={mail_user}
#SBATCH --mail-type={mail_type}
#SBATCH --partition={partition}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --ntasks={ntasks}
#SBATCH --time={time}
#SBATCH --account={account}

{bash_setup}

__script__"""


class SlurmLauncher(object):


	def __init__(self, config, template=TEMPLATE,  slurm_cmd="sbatch --export=ALL"):

		self.config = config
		self.slurm_config = config.slurm_config
		self.log_dir = config.root_dir_path.joinpath("slurm-logs").resolve().as_posix()
		self.scripts_dir = config.root_dir_path.joinpath("slurm-scripts").resolve().as_posix()
		self.logger = Logger.get_instance()

		config.root_dir_path.joinpath("slurm-logs").mkdir(exist_ok=True)
		config.root_dir_path.joinpath("slurm-scripts").mkdir(exist_ok=True)

		self.template = template
		self.slurm_cmd = slurm_cmd


	def launch(self, observation, process_cmd):
		name = observation.source + "_" + observation.obs_start_utc + ".bash"

		biggest_size = 0
		for o in observation.observation_chunks:
				biggest_size = o.file_size if o.file_size > biggest_size else biggest_size
		total_size = biggest_size * len(observation.observation_chunks) # just being conservative
		memory = "{}g".format(math.ceil(biggest_size * 28))
		ncpus = 1
		if biggest_size < 5:
			wall_time = "4:00:00"
		elif biggest_size < 10:
			wall_time = "8:00:00"
		else:
			wall_time = "24:00:00"

		directory = observation.observation_chunks[0].construct_output_path(self.config.root_dir_path, "").resolve().as_posix()
		self.logger.debug("Directory for slurm script and log: {}".format(directory))
		self.log_dir =directory
		self.scripts_dir = directory

		script = self.template.format(name=name,
										log_dir=self.log_dir,
										bash_setup=self.config.slurm_config.bash_setup,
										mail_user=self.config.slurm_config.mail_user,
										mail_type= self.config.slurm_config.mail_type,
										partition = self.config.slurm_config.partition,
										mem_per_cpu = memory,
										ntasks=ncpus,
										time=wall_time,
										account="oz005"
								).replace("__script__", process_cmd)

		script_file = Path(self.scripts_dir).joinpath(name).resolve().as_posix()

		with open(script_file , "w") as sh:
			sh.write(script)

		command = "{} {}".format(self.slurm_cmd, script_file)

		output = run_process(command)

		job_id = None

		for line in output.splitlines():
			if "Submitted batch job" in line:
				job_id = line.split()[-1]

		if job_id is None:
			self.logger.fatal("job did not launch properly {}".format(name))

		return job_id




class SlurmChecker(threading.Thread):

	stop = threading.Event()

	@staticmethod
	def signal_handler(sig, frame):
		print('You pressed Ctrl+C!, aborting')
		SlurmChecker.stop.set()

	def signal_init():
		signal.signal(signal.SIGINT, SlurmChecker.signal_handler)

		
	def __init__(self, job_ids=None):
		self.logger = Logger.get_instance()
		self.db_manager = DBManager.get_instance()
		self.job_ids = job_ids
		if self.job_ids is None:
			query = self.db_manager.get_session().query(Observation)
			query = query.filter(Observation.obs_type.in_(PULSAR_TYPES))
			query = query.filter(Observation.processed == False)
			query = query.filter(Observation.slurm_id != None)
			self.job_ids = [obs.slurm_id for obs in query.all()]
		threading.Thread.__init__(self)




	def get_job_dict(self,jobids):
		command = "sacct -Pn  -o jobid,state%50 -j {}".format(",".join([str(i) for i in jobids]))
		output = run_process(command)
		job_dict = {}
		for line in output.splitlines():
			job_id, state = line.split("|")
			job_dict[job_id] = state
		
		return job_dict
		

	def check_and_update_jobs(self):
		self.logger.debug("jobs_ids: {}".format(self.job_ids))
		if len(self.job_ids) > 0:
			job_info_dict = self.get_job_dict(self.job_ids)
			for job_id in self.job_ids[:]:
				slurm_job = self.db_manager.get_session().query(SlurmJob).filter(SlurmJob.id == job_id).first()
				new_status = job_info_dict[str(job_id)]
				self.logger.debug("Job ID: {} old status: {} new status: {}".format(
					job_id, slurm_job.state, new_status))
				if new_status != slurm_job.state:
					slurm_job.state = new_status
					self.db_manager.add_to_db(slurm_job)
					if(new_status not in ['COMPLETED','PENDING', 'RUNNING']):
						self.logger.error("Job {}  failed with status {}".format(
							job_id, new_status))
						
					if(new_status in ['COMPLETED','FAILED', 'OUT_OF_MEMORY']):
						self.job_ids.remove(job_id)			
							
		else:
			self.logger.info("No jobs to check...")
			SlurmChecker.stop.set()
			
						

	def run(self):
		while True:
			self.logger.debug("checking slurm jobs...")
			self.check_and_update_jobs()
			self.logger.debug("waiting for 120 seconds...")
			SlurmChecker.stop.wait(timeout=120)
			if SlurmChecker.stop.is_set():
				print("event is set, aborting..")
				break
		self.logger.debug("Thread stopping...")







def get_args():
	argparser = argparse.ArgumentParser(description="process pulsar data", 
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	argparser.add_argument("--config", dest="config",
	                       help="config file", required=True)

	Logger.add_logger_argparse_options(argparser)

	args = argparser.parse_args()
	return args


def main():
	args = get_args()
	logger = Logger.get_instance(args)
	config = ConfigurationReader(args.config).get_config()
	db_manager = DBManager.get_instance(config.db_file)
	SlurmChecker.signal_init()
	slurm_checker = SlurmChecker()
	slurm_checker.start()
	slurm_checker.join()
	logger.info("done")
	sys.exit(0)


if __name__ == "__main__":
	main()
