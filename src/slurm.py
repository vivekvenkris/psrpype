import sys
import os
import subprocess
import hashlib
from datetime import datetime
from constants import PULSAR_TYPES
from db_orms import DBManager, Observation, SlurmJob
from gen_utils import run_process
from pathlib import Path
from log import Logger
from threading import Thread
import time

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


class Slurm(object):


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


	def launch(self, source, obs_start_utc,process_cmd, memory, ncpus, wall_time):
		name = source + "_" + obs_start_utc + ".bash"

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


class SlurmChecker(Thread):

	def __init__(self):
		self.logger = Logger.get_instance()
		self.db_manager = DBManager.get_instance()

	def check_and_update_jobs(self):

		query = self.db_manager.get_session().query(Observation)
		query = query.filter(Observation.obs_type.in_(PULSAR_TYPES))
		query = query.filter(Observation.processed == False)
		query = query.filter(Observation.slurm_id is not None)
		job_ids = [obs.slurm_id for obs in query.all()]

		import pyslurm.pyslurm
		slurmdb_jobs = pyslurm.slurmdb_jobs()
		job_info_dict = slurmdb_jobs.get(job_ids=job_ids)

		for job_id in job_info_dict:
			slurm_job = SlurmJob.query.get(job_id)
			new_status = job_info_dict[job_id]['status_str']
			self.logger.debug("Job ID: {} old status: {} new status: {}".format(job_id, slurm_job.status, new_status))
			if new_status != slurm_job.status:
				slurm_job.status = new_status
				self.db_manager.get_session().commit()
				if(new_status not in ['COMPLETED','PENDING', 'RUNNING','TIMEOUT']):
					self.logger.error("Job {} for observation {} failed with status {}".format(
						job_id, slurm_job.observation.obs_start_utc, new_status))

	def run(self):

		while True:
			self.logger.debug("checking slurm jobs")
			self.check_and_update_jobs()
			time.sleep(120)


