from sqlalchemy import Column, Integer, String, Float,BigInteger
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

import sqlalchemy

engine = create_engine('sqlite:////Users/vkrishnan/software/psrpype/test.db', echo=True)
Base = declarative_base()

class Backends(Base):
	__tablename__="backends"

	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)
	description = Column(String)

	def __repr__(self):
		return "<Backend (name='%s'>" % (self.name)



class Telescopes(Base):
	__tablename__="telescopes"

	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)
	description = Column(String)

	def __repr__(self):
		return "<Telescopes (name='%s'>" % (self.name)


class Collections(Base):
	__tablename__="collections"

	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)
	pid = Column(String, nullable=False)
	description = Column(String)
	observations = relationship("Observations", back_populates="collections")

	def __repr__(self):
		return "<Collections (name='%s', pid='%s', description='%s'>" % (self.name, self.description)



class Obstypes(Base):
	__tablename__="obs_types"

	id = Column(Integer, primary_key=True)
	type = Column(String, nullable=False)

	def __repr__(self):
		return "<Obstypes (type='%s'>" % (self.name)

class ProcessingTypes(Base):
	__tablename__="processing_types"

	id = Column(Integer, primary_key=True)
	type = Column(String, nullable=False)

	def __repr__(self):
		return "<Obstypes (type='%s'>" % (self.name)


class Observations(Base):
	__tablename__="observations"

	id = Column(Integer, primary_key=True)

	collection_id = Column(Integer, ForeignKey('collections.id'), nullable=False)
	collection = relationship("Collections")

	telescope_id = Column(Integer, ForeignKey('telescopes.id'), nullable=False)
	telescope = relationship("Telescopes")

	backend_id = Column(Integer, ForeignKey('backends.id'), nullable=False)
	backend = relationship("Backends")

	obstype_id = Column(Integer, ForeignKey('obs_types.id'), nullable=False)
	obstype = relationship("Obstypes")


	nchan = Column(Integer, nullable=False)
	nsubint = Column(Integer, nullable=False)
	nbin = Column(Integer, nullable=False)
	npol = Column(Integer, nullable=False)
	cfreq = Column(Float, nullable=False)
	bw = Column(Float, nullable=False)
	file_size = Column(BigInteger, nullable=False)
	source = Column(String, nullable=False)
	cfreq = Column(Float, nullable=False)


class Processings(Base):
	__tablename__="processings"

	id = Column(Integer, primary_key=True)
	type = Column(String, nullable=False)
	status = Column(Integer, nullable=False)

	observation_id = Column(Integer, ForeignKey('processing_types.id'), nullable=False)
	obstype = relationship("ProcessingTypes")

	processing_type = Column(Integer, ForeignKey('processing_types.id'), nullable=False)
	obstype = relationship("ProcessingTypes")

class DBManager(object):
	_Session = None
	@staticmethod
	def init_db(db_path):
		engine = create_engine('sqlite:////{}'.format(db_path.resolve().as_posix()), echo=True)
		_Session = sessionmaker(bind=engine)	
		Base.metadata.create_all(engine, checkfirst=True)

	@property
	def get_session():
		


