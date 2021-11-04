import os
from pathlib import Path
from  gen_utils import ensure_file_exists, strip_quotes_and_spaces, guess_and_change_dtype, run_process
import shutil
from constants import POLNCAL_DIR
import numpy as np
import psrchive as ps
from log import Logger
class SlurmConfig(object):
	def __init__(self, num_simultaneous_jobs, partition, mail_user, mail_type, bash_setup):
		self._num_simultaneous_jobs = num_simultaneous_jobs
		self._partition = partition
		self._mail_type = mail_type
		self._mail_user = mail_user
		self._bash_setup = bash_setup

	@property
	def num_simultaneous_jobs(self):
		return self._num_simultaneous_jobs

	@property
	def partition(self):
		return self._partition

	@property
	def mail_type(self):
		return self._mail_type

	@property
	def mail_user(self):
		return self._mail_user

	@property
	def bash_setup(self):
		return self._bash_setup

	def __str__(self):
		return " num_simultaneous_jobs {} \n partition {} \n mail_user {}  \n mail_type {} \n".format(self.num_simultaneous_jobs, self.partition, self.mail_user, self.mail_type)

	def __repr__(self):
		return self.__str__()	
	


class Config(object):

	def __init__(self, root_dir, dm_file, rm_file, decimation_file, db_file, global_fluxcal_db, global_polncal_db, global_metm_db, rfi_tolerance, slurm_config):
		self._root_dir = root_dir
		self._slurm_config = slurm_config
		self._dm_file = dm_file
		self._rm_file = rm_file
		self._decimation_file = decimation_file
		self._db_file = db_file
		self._global_fluxcal_db = global_fluxcal_db
		self._global_polncal_db = global_polncal_db
		self._global_metm_db = global_metm_db
		self._rfi_tolerance = rfi_tolerance
		self.logger = Logger.getInstance()

		self.dms={}
		self.rms={}
		self.decimation_list={}


		dm_file_path = Path(dm_file)
		if dm_file_path.exists():
			
			dm_lines =   open(dm_file_path.resolve().as_posix(), "r" ).read().splitlines()	

			for l in dm_lines:
				if "#" in l or l == "":
					continue				
				chunks = l.strip().split()
				if(len(chunks) < 2):
					self.logger.warn("Uncommented line has incorrect values: {}".format(l))		
					continue		
				self.dms[chunks[0]] = chunks[1]
		else:
			logger.warn("DM list not found here: {}".split(dm_file_path.resolve().as_posix()))


		rm_file_path = Path(rm_file)
		if rm_file_path.exists():
			rm_lines = open(rm_file_path.resolve().as_posix(), "r" ).read().splitlines()	

			for l in rm_lines:
				if "#" in l or l == "":
					continue
				chunks = l.strip().split()
				if(len(chunks) < 2):
					self.logger.warn("Uncommented line has incorrect values: {}".format(l))
					continue
				self.rms[chunks[0]] = chunks[1]
		else:
			logger.warn("RM list not found here: {}".split(rm_file_path.resolve().as_posix()))		


		decimation_file_path = Path(decimation_file)

		if decimation_file_path.exists():
			decimation_lines = open(decimation_file_path.resolve().as_posix(), "r" ).read().splitlines()	

			for l in decimation_lines:
				if "#" in l or l == "":
					continue				
				chunks = l.strip().split(',',1)
				if(len(chunks) < 2):
					self.logger.warn("Uncommented line has incorrect values: {}".format(l))	
					continue			
				self.decimation_list[chunks[0]] = chunks[1]




	@property
	def root_dir(self):
		return self._root_dir

	@property
	def root_dir_path(self):
		return Path(self._root_dir)

	@property
	def slurm_config(self):
		return self._slurm_config

	@property
	def dm_file(self):
		return self._dm_file

	@property
	def rm_file(self):
		return self._rm_file

	@property
	def decimation_file(self):
		return self._decimation_file								

	@property
	def db_file(self):
		return self._db_file

	@property
	def global_fluxcal_db(self):
		return self._global_fluxcal_db

	@property
	def global_polncal_db(self):
		return self._global_polncal_db
		
	@property
	def global_metm_db(self):
		return self._global_metm_db		

class ConfigurationReader(object):

	@classmethod
	def _copy_pcm_files(cls, out_path):
		file_path=Path(__file__)
		resources_dir = file_path.parent.parent.joinpath("resources")
		metm_pcm_file_paths = resources_dir.glob('*_metm.pcm') # file name of the form UTC_metm.pcm

		new_file_names = []
		for metm_pcm_file_path in metm_pcm_file_paths:

			ar = ps.Archive_load(metm_pcm_file_path.resolve().as_posix())

			utc = metm_pcm_file_path.name.split("_")[0]

			pcm_dest_path = out_path.joinpath(POLNCAL_DIR).joinpath("P999").joinpath(ar.get_source()).joinpath("00000").joinpath(utc).joinpath(str(ar.get_centre_frequency()))
			pcm_dest_path.mkdir(parents=True, exist_ok=True)

			new_file_name = pcm_dest_path.joinpath(metm_pcm_file_path.name).resolve().as_posix()
			shutil.copy(metm_pcm_file_path.resolve().as_posix(), new_file_name)

			new_file_names.append(new_file_name)

			del ar 		

		np.savetxt(out_path.joinpath(POLNCAL_DIR).joinpath("pcm.files").resolve().as_posix(),new_file_names,delimiter=" ", fmt="%s")

		global_metm_db_path = out_path.joinpath("global_metm.db")
		command = "pac -k {} -W {}".format(global_metm_db_path.resolve().as_posix(), 
			out_path.joinpath(POLNCAL_DIR).joinpath("pcm.files").resolve().as_posix())
		run_process(command)

		return global_metm_db_path


	@classmethod
	def init_default(cls, out_path):

		config_dest_path = out_path.joinpath("default.cfg")
		file_path=Path(__file__)
		src = file_path.parent.parent.joinpath("configs").joinpath("default.cfg")
		line = None

		global_fluxcal_db_path =  out_path.joinpath("global_fluxcal.db")
		with open(global_fluxcal_db_path.resolve().as_posix(), 'w') as f:
			f.write("Pulsar::Database::path {} \n".format(out_path.parent.resolve().as_posix()))

		global_polncal_db_path =  out_path.joinpath("global_polncal.db")			
		with open(global_polncal_db_path.resolve().as_posix(), 'w') as f:
			f.write("Pulsar::Database::path {} \n".format(out_path.parent.resolve().as_posix()))

		db_path = out_path.joinpath("psrpype.sqlite3")

		global_metm_db_path = ConfigurationReader._copy_pcm_files(out_path)


		with open(src) as f:
			lines = (f
						.read()
						.replace("<ROOT_OUTPUT_DIR>", out_path.resolve().as_posix())
						.replace("<GLOBAL_FLUXCAL_DB>", global_fluxcal_db_path.resolve().as_posix())
						.replace("<GLOBAL_POLNCAL_DB>", global_polncal_db_path.resolve().as_posix())
						.replace("<GLOBAL_METM_DB>", global_metm_db_path.resolve().as_posix())
						.replace("<DB_FILE>", db_path.resolve().as_posix()))



		with open(config_dest_path, "w") as f:
			f.write(lines)

	def __init__(self, config_file_name):
		
		self._config_file_name = config_file_name
		self.dict_process_config={}
		ensure_file_exists(config_file_name)

				
		config_file = open( config_file_name, "r" )

		with open( config_file_name, "r" ) as f:
			lines =  f.read().splitlines()


		for line in lines:

			if not line or line.startswith("#"):
				continue;
			chunks = line.split('#')[0].strip().split(None,1)
			key = chunks[0]
			val = guess_and_change_dtype(strip_quotes_and_spaces(chunks[1])) if len(chunks) > 1 else ""

			self.dict_process_config[key] = val  #Save parameter key and value in the dictionary 
			  
		config_file.close()

		slurm_config =SlurmConfig(self.dict_process_config['N_SIMULTANEOUS_JOBS'],
						  self.dict_process_config['PARTITION'],
						  self.dict_process_config['MAIL_USER'],
						  self.dict_process_config['MAIL_TYPE'],
						  self.dict_process_config['SLURM_BASH_HEADER']) 

		self._config =  Config(self.dict_process_config['PSRPYPE_ROOT'], 
							self.dict_process_config['DM_LIST'],
							self.dict_process_config['RM_LIST'],
							self.dict_process_config['DECIMATION_LIST'],
							self.dict_process_config['DB_FILE'],
							self.dict_process_config['GLOBAL_FLUXCAL_DB'],
							self.dict_process_config['GLOBAL_POLNCAL_DB'],
							self.dict_process_config['GLOBAL_METM_DB'],
							self.dict_process_config['RFI_ZAP_TOLERANCE'],
							slurm_config)


	def get_config(self):
		return self._config;















		






