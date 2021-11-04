from sqlalchemy import Column, Integer, String, Float,BigInteger
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from pathlib import Path
from log import Logger
from gen_utils import get_utc_string
from constants import FLUX_CALIBRATOR_SOURCES, FLUXCAL_DIR, POLNCAL_DIR, PULSAR_DIR, SCRATCH_DIR
from exceptions import IncorrectInputsException
import numpy as np


Base = declarative_base()

class Collection(Base):
	__tablename__="collections"

	id = Column(Integer, primary_key=True)
	collection_name = Column(String, nullable=False)
	collection_path = Column(String, nullable=False)
	pid = Column(String, nullable=False)
	description = Column(String)
	observations = relationship("Observation", back_populates="collection")
	name_alias = Column(String, nullable=False)

	def __repr__(self):
		return "<Collections (name='%s', pid='%s', description='%s'>" % (self.collection_name, self.description)


class SlurmJob(Base):
	__tablename__="slurm_jobs"

	id=Column(Integer, primary_key=True)
	observations = relationship("Observation", back_populates="slurm_job")
	state = Column(String, nullable=False)
	description = Column(String)


class Observation(Base):
	__tablename__="observations"

	id = Column(Integer, primary_key=True)

	nchan = Column(Integer, nullable=False)
	nsubint = Column(Integer, nullable=False)
	nbin = Column(Integer, nullable=False) 
	npol = Column(Integer, nullable=False)
	cfreq = Column(Float, nullable=False)
	bw = Column(Float, nullable=False)
	file_size = Column(BigInteger, nullable=False)
	source = Column(String, nullable=False)
	cfreq = Column(Float, nullable=False)
	backend = Column(String, nullable=False)
	telescope = Column(String, nullable=False)
	start_mjd = Column(Float, nullable=False)
	obs_start_utc = Column(String, nullable=False)
	end_mjd = Column(Float, nullable=False)
	file_size=Column(BigInteger, nullable=False)
	obs_type = Column(String, nullable=False)
	original_file = Column(String, nullable=False)
	sym_file = Column(String, nullable=False)
	preprocessed_file = Column(String)
	cleaned_file = Column(String)
	calibrated_file = Column(String)
	recleaned_file = Column(String)

	def __init__(self, ar, original_file_path = None, sym_file_path = None, obs_start_utc = None):
		self.nchan = ar.get_nchan()
		self.nsubint = ar.get_nsubint()
		self.nbin = ar.get_nbin()
		self.npol = ar.get_npol()
		self.cfreq =ar.get_centre_frequency()
		self.bw = ar.get_bandwidth()
		self.file_size = Path(ar.get_filename()).stat().st_size/1e9
		self.source = ar.get_source()
		self.backend = ar.get_backend_name()
		self.telescope = ar.get_telescope()
		self.start_mjd = ar.start_time().in_days()
		self.end_mjd = ar.end_time().in_days()
		self.obs_type = ar.get_type()
		self.obs_start_utc = obs_start_utc
		self.original_file = original_file_path.resolve().as_posix() if original_file_path is not None else None
		self.sym_file = sym_file_path.resolve().as_posix() if sym_file_path is not None else None

	collection_id = Column(Integer, ForeignKey('collections.id'), nullable=False)
	collection = relationship("Collection")

	slurm_id = Column(Integer, ForeignKey('slurm_jobs.id'))
	slurm_job = relationship("SlurmJob")

	def __repr__(self):
		return "<Observation (source = {},start_utc={}, original_file = {}, type= {})>\n".format(self.source, self.obs_start_utc, self.original_file, self.obs_type)

	def is_flux_cal(self):
		return self.source in FLUX_CALIBRATOR_SOURCES

	def is_poln_cal(self):
		return self.source not in FLUX_CALIBRATOR_SOURCES and self.obs_type == 'PolnCal'

	def is_pulsar(self):
		return self.obs_type == 'Pulsar'

	def construct_output_path(self,root_dir_path, process_str):
		if self.is_flux_cal():
			out_root_dir = FLUXCAL_DIR
		elif self.is_poln_cal():
			out_root_dir = POLNCAL_DIR
		else:
			out_root_dir = PULSAR_DIR

		out_path = (root_dir_path.joinpath(out_root_dir)
										.joinpath(self.collection.pid)
										.joinpath(self.source)
										.joinpath(self.collection.collection_name)
										.joinpath(self.obs_start_utc)
										.joinpath(str(self.cfreq))
										.joinpath(process_str))

		out_path.mkdir(parents=True, exist_ok=True)

		return out_path

	def construct_output_archive_name(self, process_str):
		obs_sym_path =  Path(self.sym_file)
		out_name = self.source + "_" + obs_sym_path.stem + "_" + process_str + "".join(obs_sym_path.suffixes)
		return out_name


	def construct_output_archive_path(self,root_dir_path, process_str):

		out_path = self.construct_output_path(root_dir_path, process_str)
		out_file_path = Path(out_path).joinpath(self.construct_output_archive_name(process_str))

		return out_file_path





class DBManager(object):

	__instance = None

	@staticmethod
	def get_instance(db_path = None):
		logger = Logger.getInstance()

		""" Static access method. """
		if DBManager.__instance is None:
			logger.debug("creating new DB manager instance..")
			DBManager.__instance = DBManager(db_path)
		else:
			logger.debug("Using existing DB Manager instance..")

		return DBManager.__instance


	def __init__(self, db_path):

		if db_path is None:
			raise IncorrectInputsException("DB path is none")

		elif type(db_path) is str:
			db_path = Path(db_path)

		db_path = db_path.resolve().as_posix()
		open(db_path, 'a').close()

		self.logger = Logger.getInstance()
		self.logger.debug("Opening {} ".format(db_path ))

		self._engine = create_engine('sqlite+pysqlite:////{}'.format(db_path), echo=False, future=True)
		self._Session = sessionmaker(bind=self._engine)	
		self._current_session = None
		

	def init_database(self):
		Base.metadata.create_all(self._engine, checkfirst=True)


	def add_to_db(self, objs):
		session = self.get_session()

		if type(objs) == list or type(objs) == np.ndarray:
			for x in objs:
				session.add(x) 
		else:
			session.add(objs)

		session.commit()


	def get_session(self):
		if self._current_session is None:
			self.start_session()
		return self._current_session


	def start_session(self):
		self._current_session =  self._Session()
		return self._current_session

	def close_session(self):
		self._current_session.close()

	def __del__(self):
		if self._current_session is not None:
			self.close_session()





