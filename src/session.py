from log import Logger
from processor import Processor
from gen_utils import run_process
from db_orms import DBManager
from pathlib import Path

class ObservingSession(object):

	def __init__(self, config, observation):
		self.observation = observation
		self.observation_chunks = observation.observation_chunks
		self.logger = Logger.get_instance()
		self.db_manager = DBManager.get_instance()
		self.config = config

	def process(self):
		for observation_chunk in self.observation_chunks:

			self.logger.debug("considering {}".format(observation_chunk))

			processor = Processor(self.config, observation_chunk)

			processor.preprocess()
			processor.clean()
			processor.calibrate()
			processor.reclean()

			observation_chunk.processed = True
			self.db_manager.add_to_db(observation_chunk)

		
		self._consolidate()


	def _consolidate(self):
		psradded_file = self._psradd()
		self._decimate(psradded_file)

	def _psradd(self):
		self.logger.debug("Psradding...")
		files = " ".join([o.recleaned_file for o in self.observation_chunks])
		out_file = self.observation_chunks[0].construct_output_path(self.config.root_dir_path, "").joinpath(
			self.observation_chunks[0].source + "_" + self.observation_chunks[0].obs_start_utc + "_psradd.rf").resolve().as_posix()

		if Path(out_file).exists():
			self.logger.debug("{} already exists, skipping...".format(out_file))
			return out_file

		command = "psradd -o {} {}".format(out_file, files)
		self.logger.debug("Running {}".format(command))
		run_process(command)
		for o in self.observation_chunks:
			o.psradded_file = out_file

		self.db_manager.add_to_db(self.observation_chunks)

		return out_file

	def _decimate(self, psradded_file):

		if(self.observation.decimated):
			self.logger.debug("Decimated files already exists, skipping...")
			return

		self.logger.debug("Decimating...")
		if self.observation_chunks[0].source in self.config.decimation_list:

			decimation_list = self.config.decimation_list[self.observation_chunks[0].source]
			decimated_file_path = self.observation_chunks[0].construct_output_path(
														self.config.root_dir_path, "decimated")
			for flags in decimation_list.strip().split(","):
				command = "pam {} {} -u {}".format(
					flags, psradded_file, decimated_file_path.resolve().as_posix())
				self.logger.debug("Running {}".format(command))
				run_process(command)
				self.logger.debug("decimated file: {}".format(decimated_file_path.resolve().as_posix()))

			self.logger.debug(" All decimations done successfully...")
		
			self.observation.decimated = True
			self.db_manager.add_to_db(self.observation)

		else:
			self.logger.warn("No decimation information for {}, skipping...".format(
				self.observation_chunks[0].source))


