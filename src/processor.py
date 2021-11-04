from log import Logger
from db_orms import *
from cal_utils import CalUtils
from rfi_utils import Cleaner
from gen_utils import run_process
import shutil

class Processor(object):

	def __init__(self, config, observation):
		self.observation = observation
		self.config = config
		self.logger = Logger.getInstance()
		self.db_manager = DBManager.get_instance()
		self.cleaner = 	cleaner = Cleaner(self.config, 'clfd')


	def preprocess(self):

		if self.observation.preprocessed_file is not None:
			self.logger.warn("Preprocessed file exists, skipping...")
			return

		flags = ""

		if self.observation.source in self.config.dms:
			flags = flags + " -d " + self.config.dms[self.observation.source] 

		if  self.observation.source in self.config.rms:
			flags = flags + " -R " + self.config.rms[self.observation.source] + ""

		ephemeris_path = self.config.root_dir_path.joinpath("ephemeris").joinpath(self.observation.source + ".par")

		if ephemeris_path.exists():
			flags = flags +  " -E " + ephemeris_path.resolve().as_posix()

		if self.observation.bw < 0:
			flags = flags + " --reverse_freqs "

		preprocessed_dir_path = self.observation.construct_output_path(self.config.root_dir_path, "preprocessed")
		preprocessed_file_name = self.observation.construct_output_archive_name("preprocessed")

		if flags != "":
			command = "pam {} {} -u {} ".format(flags, self.observation.original_file, preprocessed_dir_path.resolve().as_posix())
			run_process(command)
			shutil.move(preprocessed_dir_path.joinpath(Path(self.observation.original_file).name), preprocessed_dir_path.joinpath(preprocessed_file_name))
			self.observation.preprocessed_file = preprocessed_dir_path.joinpath(preprocessed_file_name).resolve().as_posix()
			self.logger.info("preprocessing done successfully...")

		else:
			self.logger.warn("Nothing to preprocess for {}".format(self.observation.sym_file))

		self.db_manager.add_to_db(self.observation)


	def _clean(self, name):

		self.cleaner.clean_and_save(self.observation, name)
		self.db_manager.add_to_db(self.observation)

	def clean(self):
		if self.observation.cleaned_file is not None:
			self.logger.warn("Cleaned file exists, skipping...")		
			return
		self._clean("cleaned")
		self.logger.info("cleaning done successfully...")


	def reclean(self):
		if self.observation.recleaned_file is not None:
			self.logger.warn("Cleaned file exists, skipping...")	
			return			
		self._clean("recleaned")
		self.logger.info("recleaning done successfully...")


	def calibrate(self):

		if self.observation.calibrated_file is not None:
			self.logger.warn("Calibrated file exists, skipping...")		
			return

		calibrated_dir_path = self.observation.construct_output_path(self.config.root_dir_path, "calibrated")
		command = "pac -k 3.0 -g -S -T -O {} -d {} -d {} -d {} {}".format(calibrated_dir_path.resolve().as_posix(), 
																			self.config.global_fluxcal_db, 
																			self.config.global_polncal_db, 
																			self.config.global_metm_db,
																			self.observation.cleaned_file)

		run_process(command)
		ext = Path(self.observation.cleaned_file).suffix
		calibrated_file_name = Path(self.observation.cleaned_file).name.replace(ext, "calib")
		new_file_name = self.observation.cleaned_file.replace("cleaned", "calibrated")

		shutil.move(calibrated_dir_path.joinpath(calibrated_file_name).resolve().as_posix(), calibrated_dir_path.joinpath(new_file_name))
		self.observation.calibrated_file = new_file_name
		self.db_manager.add_to_db(self.observation)
		self.logger.info("calibration done successfully...")


	def decimate(self):

		if self.observation.source in self.config.decimation_list:

			decimation_list = self.config.decimation_list[self.observation.source]
			decimated_file_path = self.observation.construct_output_path(self.config.root_dir_path, "decimated")
			for flags in decimation_list.strip().split(","):
				command = "pam {} {} -u {}".format(flags, self.observation.recleaned_file, decimated_file_path.resolve().as_posix())
				run_process(command)

		else:
			self.logger.warn("No decimation information for {}, skipping...".format(self.observation.source))




	




