import os
from pathlib import Path
from  gen_utils import ensure_file_exists, strip_quotes_and_spaces, guess_and_change_dtype


class SlurmConfig(object):
	def __init__(self, num_simultaneous_jobs, partition, mail_user, mail_type):
		self._num_simultaneous_jobs = num_simultaneous_jobs
		self._partition = partition
		self._mail_type = mail_type
		self._mail_user = mail_user

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

	def __str__(self):
		return " num_simultaneous_jobs {} \n partition {} \n mail_user {}  \n mail_type {} \n".format(self.num_simultaneous_jobs, self.partition, self.mail_user, self.mail_type)

	def __repr__(self):
		return self.__str__()	
	


class config(object):

	def __init__(root_dir, dm_file, rm_file, decimation_file, slurm_config):
		self._root_dir = root_dir
		self._slurm_config = slurm_config
		self._dm_file = dm_file
		self._rm_file = rm_file
		self._decimation_file = decimation_file

	@property
	def root_dir():
		return _root_dir

	@property
	def slurm_config():
		return _slurm_config

	@property
	def dm_file():
		return _dm_file

	@property
	def rm_file():
		return _rm_file

	@property
	def decimation_file():
		return _decimation_file								


class ConfigurationReader(object):

	@staticmethod
	def init_default(out_path):
		dest = out_path.joinpath("default.cfg")
		file_path=Path(__file__)
		src = file_path.parent.parent.joinpath("configs").joinpath("default.cfg")
		line = None
		with open(src) as f:
			line = f.read().replace("<ROOT_OUTPUT_DIR>", dest.resolve().as_posix())

		with open(dest, "w") as f:
			f.write(line)

		 


	def __init__(self, config_file_name):
		
		self._config_file_name = config_file_name
		self.dict_process_config={}
		ensure_file_exists(config_filename)

				
		config_file = open( config_filename, "r" )

		with open( config_filename, "r" ) as f:
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
						  self.dict_process_config['MAIL_TYPE']) 

		self._config =  Config(self.dict_process_config['PSRPYPE_ROOT'], 
							self.dict_process_config['DM_LIST'],
							self.dict_process_config['RM_LIST'],
							self.dict_process_config['DECIMATION_LIST'],
							slurm_config)

		@property
		def config(self):
			return self._config;















		






