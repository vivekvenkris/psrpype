
import pandas as pd
from utils import ensure_directory_exists, ensure_file_exists, file_exists, is_file_empty, write_array_pretty
import constants
import exceptions
import numpy as np
from log import Logger 
class LedgerEntry(object):
	
	def __init__(self, header_chunks, info_chunks):
		self.rawfilename = info_chunks[header_chunks.index(constants.RAW_FILE)] if constants.RAW_FILE in header_chunks else None
		self.nchan = info_chunks[header_chunks.index(constants.NCHAN)]if constants.NCHAN in header_chunks else None
		self.nsubint = info_chunks[header_chunks.index(constants.NSUBINT)] if constants.NSUBINT in header_chunks else None
		self.npol = info_chunks[header_chunks.index(constants.NPOL)] if constants.NPOL in header_chunks else None
		self.cfreq = info_chunks[header_chunks.index(constants.CFREQ)] if constants.CFREQ in header_chunks else None
		self.bw = info_chunks[header_chunks.index(constants.BW)] if constants.BW in header_chunks else None
		self.backend = info_chunks[header_chunks.index(constants.BACKEND)] if constants.BACKEND in header_chunks else None
		self.telescope = info_chunks[header_chunks.index(constants.TELESCOPE)] if constants.TELESCOPE in header_chunks else None
		self.type = info_chunks[header_chunks.index(constants.TYPE)] if constants.TYPE in header_chunks else None
		self.file_size = info_chunks[header_chunks.index(constants.FILE_SIZE)] if constants.FILE_SIZE in header_chunks else None
		self.source = info_chunks[header_chunks.index(constants.SOURCE)] if constants.SOURCE in header_chunks else None
		self.status = "0"
		self.info_chunks = info_chunks

		self.logger = Logger.getInstance().logger


	def __str__(self):
		return str(self.info_chunks)

	def __eq__(self, other):
		if isinstance(other, self.__class__):
			return self.rawfilename == other.rawfilename
		else:
			return False

	def __cmp__(self, other):
		if isinstance(other, self.__class__):
			print(self.rawfilename, other.rawfilename, self.rawfilename == other.rawfilename)
			return self.rawfilename in other.rawfilename
		else:
			return False

	def __ne__(self, other):
	    return not self.__eq__(other)
 
	def __hash__(self):
		print("d")
		return self.rawfilename


	@property
	def array(self):
	 	arr = self.info_chunks
	 	arr.append(self.status)
	 	return arr
	 



class Ledger(object):


	def __init__(self, ledger_file):
		self.ledger_file = ledger_file
		self.logger = Logger.getInstance().logger
		self.header = None
	 	self.entries = []
		if( file_exists(ledger_file) and not is_file_empty(ledger_file)):
			self.logger.info("Loading existing ledger file")
			self.header, self.entries = self.get_entries_from_file(ledger_file)

	def add_new_entries(self, info_file, sources=None):
		header, entries = self.get_entries_from_file(info_file)
		shortlisted_entries = entries if sources is None else [x for x in entries if x.source in sources]

		if self.entries is not None:
			print(len(self.entries))

		if shortlisted_entries is not None:
			for i in shortlisted_entries:
				if(not any(i == a for a in self.entries)):
					self.entries.append(i)
				else:
					self.logger.warn("Skipping duplicate: " + str(i.array))


		# self.entries = 	np.column_stack((self.entries, shortlisted_entries)) if self.entries is not None else shortlisted_entries
		if self.entries is not None:
			print(len(self.entries))



		if(self.header is None):
			self.header = header.rstrip() + " status\n"


	def write_ledger(self, new_ledger_file = None):

		file = self.ledger_file if new_ledger_file is None else new_ledger_file

		array_to_write = []

		array_to_write.append(self.header.strip().split())


		for entry in self.entries:
			array_to_write.append(entry.array)


		write_array_pretty(file, array_to_write, self.logger)


	def get_entries_from_file(self, file):

		ensure_file_exists(file)


		with open(file) as f:
				 lines = f.readlines()

		headers = [x for x in lines if constants.RAW_FILE in x]
		if len(headers) !=1:
			self.logger.info("Headers:" + str(headers))
			raise IncorrectFileHeaderException(file)

		header = headers[0]

		header_chunks = header.strip().split()

		for line in lines:
			if 'Medusa' not in line and '#' not in line:
				self.logger.warn("skipping non Medusa data: " + line) 

		return header, [LedgerEntry(header_chunks, line.strip().split()) for line in lines if "#" not in line and 'Medusa' in line]
		
	
	def __str__(self):
		return "\n".join([str(i) for i in self.entries])

   	







   				












	

		

