from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlalchemy

engine = create_engine('sqlite:////Users/vkrishnan/software/psrpype-uwl/test.db', echo=True)
Base = declarative_base(bind=engine)
Session = sessionmaker(bind=engine)

class Backends(Base):
	__tablename__="backends"

	id = Column(Integer, primary_key=True)
	name = Column(String)

	def __repr__(self):
		return "<Backend (name='%s'>" % (self.name)



class Telescopes(Base):
	__tablename__="telescopes"

	id = Column(Integer, primary_key=True)
	name = Column(String)

	def __repr__(self):
		return "<Telescopes (name='%s'>" % (self.name)


class Collections(Base):
	__tablename__="collections"

	id = Column(Integer, primary_key=True)
	name = Column(String)
	description = Column(String)

	def __repr__(self):
		return "<Collections (name='%s', description='%s'>" % (self.name, self.description)



class Obstypes(Base):
	__tablename__="obstypes"

	id = Column(Integer, primary_key=True)
	type = Column(String)

	def __repr__(self):
		return "<Obstypes (type='%s'>" % (self.name)




if __name__ == '__main__':
	session = Session()
	Base.metadata.create_all(engine)