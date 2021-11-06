from log import Logger
from db_orms import *
from cal_utils import CalUtils
from rfi_utils import Cleaner
from gen_utils import run_process
import shutil

class Processor(object):

	def __init__(self, config, observation_chunk):
		self.observation_chunk = observation_chunk
		self.config = config
		self.logger = Logger.get_instance()
		self.db_manager = DBManager.get_instance()
		self.cleaner = 	cleaner = Cleaner(self.config, 'clfd')

	def fix_cal_type(self):

		if self.observation_chunk.is_flux_cal() and "FluxCal" not in self.observation_chunk.obs_type : 
		
			obs_type = "FluxCal-On" if "_O" in self.observation_chunk.source else "FluxCal-Off"
			command = "psredit -c type={} -m {}".format(obs_type, self.observation_chunk.original_file)
			self.observation_chunk.obs_type = obs_type
			run_process(command)

	def preprocess(self):

		if self.observation_chunk.preprocessed_file is not None:
			self.logger.warn("Preprocessed file exists, skipping...")
			return

		flags = ""

		# if the observation is a pulsar, then also change dm, rm and ephemeris,if they need to be updated.
		if self.observation_chunk.is_pulsar():
		
			if self.observation_chunk.source in self.config.dms:
				flags = flags + " -d " + self.config.dms[self.observation_chunk.source] 

			if  self.observation_chunk.source in self.config.rms:
				flags = flags + " -R " + self.config.rms[self.observation_chunk.source] + ""

			ephemeris_path = self.config.root_dir_path.joinpath("ephemeris").joinpath(self.observation_chunk.source + ".par")

			if ephemeris_path.exists():
				flags = flags +  " -E " + ephemeris_path.resolve().as_posix()

		else:
			self.fix_cal_type()

		if self.observation_chunk.bw < 0:
			flags = flags + " --reverse_freqs "

		preprocessed_dir_path = self.observation_chunk.construct_output_path(self.config.root_dir_path, "preprocessed")
		preprocessed_file_name = self.observation_chunk.construct_output_archive_name("preprocessed")

		if flags != "":

			command = "pam {} {} -u {} ".format(flags, self.observation_chunk.original_file, preprocessed_dir_path.resolve().as_posix())
			run_process(command)

			self.logger.debug("Moving {} to {}".format(preprocessed_dir_path.joinpath(Path(self.observation_chunk.original_file).name), preprocessed_dir_path.joinpath(preprocessed_file_name)))
			shutil.move(preprocessed_dir_path.joinpath(Path(self.observation_chunk.original_file).name), preprocessed_dir_path.joinpath(preprocessed_file_name))

			self.observation_chunk.preprocessed_file = preprocessed_dir_path.joinpath(preprocessed_file_name).resolve().as_posix()
			
			self.logger.info("preprocessing done successfully...")

		else:
			self.logger.warn("Nothing to preprocess for {}".format(self.observation_chunk.sym_file))

		self.db_manager.add_to_db(self.observation_chunk)


	def _clean(self, name):

		self.cleaner.clean_and_save(self.observation_chunk, name)
		self.db_manager.add_to_db(self.observation_chunk)

	def clean(self):
		if self.observation_chunk.cleaned_file is not None:
			self.logger.warn("Cleaned file exists, skipping...")		
			return
		self._clean("cleaned")
		self.logger.info("cleaning done successfully...")


	def reclean(self):
		if self.observation_chunk.recleaned_file is not None:
			self.logger.warn("Recleaned file exists, skipping...")	
			return			
		self._clean("recleaned")
		self.logger.info("recleaning done successfully...")


	def calibrate(self):

		if self.observation_chunk.calibrated_file is not None:
			self.logger.warn("Calibrated file exists, skipping...")		
			return

		calibrated_dir_path = self.observation_chunk.construct_output_path(self.config.root_dir_path, "calibrated")
		command = "pac -k 3.0 -S -T -O {} -d {} -d {} -d {} {}".format(calibrated_dir_path.resolve().as_posix(), 
																			self.config.global_fluxcal_db, 
																			self.config.global_polncal_db, 
																			self.config.global_metm_db,
																			self.observation_chunk.cleaned_file)

		run_process(command)
		ext = Path(self.observation_chunk.cleaned_file).suffix
		calibrated_file_name = Path(self.observation_chunk.cleaned_file).name.replace(ext, ".calib")
		new_file_name = self.observation_chunk.cleaned_file.replace("cleaned", "calibrated")

		shutil.move(calibrated_dir_path.joinpath(calibrated_file_name).resolve().as_posix(), calibrated_dir_path.joinpath(new_file_name))
		self.observation_chunk.calibrated_file = new_file_name
		self.db_manager.add_to_db(self.observation_chunk)
		self.logger.info("calibration done successfully...")


