import numpy as np
from gen_utils import run_process
from pathlib import Path
from constants import *
from log import Logger
class CalUtils(object):

	def __init__(self, config):
		self.config = config
		self.global_fluxcal_db = config.global_fluxcal_db
		self.global_polncal_db = config.global_polncal_db
		self.logger = Logger.get_instance()


	def add_to_db(self, global_db, local_db):
		self.logger.debug("Adding {} to {}".format(local_db, global_db))
		#Read local DB 
		local_db_lines = open(local_db).read().splitlines()
		shortlisted_local_db_lines = [l for l in  local_db_lines if "Pulsar::Database" not in l ]
		path = [l for l in  local_db_lines if "Pulsar::Database:path" in l ]

		#Make files absolute path, if required
		if len(path) == 1:
			shortlisted_local_db_lines = [ path[0]+"/"+l for l in shortlisted_local_db_lines]

		#check if they are not already in global DB
		global_db_lines = open(global_db).read().splitlines()
		shortlisted_local_db_lines = [l for l in shortlisted_local_db_lines if l not in global_db_lines]		

		# if there are new lines, add them to global DB
		if len(shortlisted_local_db_lines) != 0:

			self.logger.info("Adding {} lines from {} to {}".format(len(shortlisted_local_db_lines), local_db, global_db))

			with open(global_db, 'a') as f:
				f.write('\n'.join(shortlisted_local_db_lines))
				f.write('\n') #new line so the next write does not start on the same line

		else:
			self.logger.warn("No new lines to add from {} to {}".format(local_db, global_db))


	def add_to_fluxcal_db(self, local_db_file, cfreq):
		self.get_fluxcal_solutions(local_db_file, cfreq)
		self.add_to_db(self.global_fluxcal_db, local_db_file)


	def add_to_polncal_db(self, local_db_file):
		self.add_to_db(self.global_polncal_db, local_db_file)

	def get_fluxcal_solutions(self, local_db_file, cfreq):
		out_dir_path = self.config.root_dir_path.joinpath(FLUXCAL_SOLUTIONS_DIR).joinpath(str(cfreq))
		out_dir_path.mkdir(exist_ok=True, parents=True)
		command = "fluxcal -K 3.0 -d {} -O {}".format(local_db_file,out_dir_path.resolve().as_posix())
		self.logger.debug("Running command: {}".format(command))
		run_process(command)



	def create_cal_db(self, archive_list, list_file_name):

		scratch_dir_path = self.config.root_dir_path.joinpath(SCRATCH_DIR)
		list_file_path = self.config.root_dir_path.joinpath(SCRATCH_DIR).joinpath(list_file_name)

		

		np.savetxt(list_file_path.resolve().as_posix(),	np.array(archive_list), delimiter=" ", newline = "\n", fmt="%s")

		db_name = (self.config.root_dir_path
					.joinpath(SCRATCH_DIR)
					.joinpath(list_file_name.
						replace("txt","db"))
					.resolve()
					.as_posix())
		
		command = ("pac -K 3.0 -W {} -k {}"
					.format(list_file_path.resolve().as_posix(), db_name))
		self.logger.debug("Running command: {}".format(command))
		run_process(command)
		return db_name






