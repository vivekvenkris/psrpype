import glob, argparse, datetime, sys
import numpy as np
from pathlib import Path, PurePath
import psrchive as ps
from log import Logger
import numpy.lib.recfunctions as rfn
from config_parser import ConfigurationReader
from db_orms import DBManager, Collection, Observation, ObservationChunk
from gen_utils import get_utc_string
from itertools import groupby

TIMES_FILE="times.dat"
TOLERANCE=0.00010 # < 10 seconds in MJD


def get_args():
	argparser = argparse.ArgumentParser(description="Add new DAP data to DB", 
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	mutually_exclusive = argparser.add_argument_group('Mutually exclusive arguments')

	group = mutually_exclusive.add_mutually_exclusive_group(required=True)
	group.add_argument("-d", dest="dirs", help="comma separated list of PID DIR ALT_NAME", default=argparse.SUPPRESS)
	group.add_argument("--dir_list", dest="dir_list", help="a list file containing PID DIR ALT_NAME per line", default=argparse.SUPPRESS)

	argparser.add_argument("-e", "--extensions", dest="extensions", help="comma separated extensions of archive files to use", default=".ar,.cf,.rf,.zcf,.zrf")
	argparser.add_argument("-b", "--backends", dest="backends", help="comma separated backends list to process, (default: ALL)")
	argparser.add_argument("-s", "--sources", dest="sources", help="comma separated sources list to process, (default: ALL)")
	argparser.add_argument("-c", "--centre_frequencies", dest="frequencies", help="comma separated centre frequencies list to process, (default: ALL)")
	argparser.add_argument("--config", dest="config", help="config file", required=True)

	Logger.add_logger_argparse_options(argparser)



	args = argparser.parse_args()
	return args


class FileInfo(object):
	def __init__(self, file_name, archive):
		self._file_name = file_name
		self._backend = archive.get_backend_name()
		self._source =  archive.get_source()
		self._cfreq =  archive.get_centre_frequency()
		self._start_mjd = archive.start_time().in_days()
		self._end_mjd = archive.end_time().in_days()
		self._observation_chunk = ObservationChunk(archive)

	@property
	def file_name(self):
		return self._file_name

	@property
	def backend(self):
		return self._backend

	@property
	def source(self):
		return self._source

	@property
	def cfreq(self):
		return self._cfreq

	@property
	def start_mjd(self):
		return self._start_mjd

	@property
	def end_mjd(self):
		return self._end_mjd

	@property
	def observation_chunk(self):
		return self._observation_chunk		

	def __repr__(self):
		return self.__str__()

	def __str__(self):
		return "{} {} {} {} {} {} \n".format(self.file_name, self.backend, self.source, self.cfreq, self.start_mjd, self.end_mjd)


def get_file_infos(file_list, backends, sources, frequencies):
	logger = Logger.get_instance()
	file_infos = []
	float_freqencies = np.array(frequencies, dtype=np.float)

	for file in file_list:
		ar = ps.Archive_load(file)

		if(backends is not None and ar.get_backend_name() not in backends):
			logger.debug("skipping {} as {} is not in backends list".format(ar.get_filename(),ar.get_backend_name() ))
			continue

		if(sources is not None and ar.get_source() not in sources):
			logger.debug("skipping {} as {} is not in sources list".format(ar.get_filename(),ar.get_source() ))
			continue

		if(frequencies is not None and not np.any(np.isclose(ar.get_centre_frequency(),float_freqencies))):
			logger.debug("skipping {} as {} is not in centre freq list".format(ar.get_filename(),ar.get_centre_frequency() ))			
			continue	

		file_infos.append(FileInfo(file, ar))
		logger.debug("adding {} {} {}".format(ar.get_source(),ar.get_backend_name(),ar.get_centre_frequency()))

		del ar
	logger.debug("{} files added".format(len(file_infos)))
	return file_infos



def get_times_for_cfreq(file_name, cfreq):
	times = np.loadtxt(file_name, dtype={'names': ('cfreq', 'mjd_start', 'utc_start', 'mjd_end', 'utc_end', 'utc_dir'),
	                     'formats': (np.float, np.float, 'S32',  np.float, 'S32', 'S32')}) # UTC_START CFREQ MJD_END
	times_for_cfreq = times[ times['cfreq'] == cfreq ] 
	return times_for_cfreq


def main():

	# get arguments, and with that initialise the logger, and obtain the config file and the DB session
	args = get_args()
	logger = Logger.get_instance(args)
	config = ConfigurationReader(args.config).get_config()
	db_manager = DBManager.get_instance(config.db_file)
	session = db_manager.get_session()

	#get pid_list, dap_id_list, alt_name_list from input
	pid_list, dap_id_list, alt_name_list  = None, None, None

	if hasattr(args, 'dir_list'):
		dir_list_arr = np.loadtxt(args.dir_list, 
			dtype={'names': ('pid', 'dir', 'alt_name'),
			'formats': ('S128','S128', 'S128')})
		pid_list = [x.decode() for x in dir_list_arr['pid']]
		dap_id_list = [x.decode() for x in dir_list_arr['dir']]
		alt_name_list = [x.decode() for x in dir_list_arr['alt_name']]
	else:
		dir_list_arr = np.array([x.split() for x in  args.dirs.split(",")])
		pid_list =  dir_list_arr[:,0]
		dap_id_list = dir_list_arr[:,1]
		alt_name_list = dir_list_arr[:,2]



	in_dir_paths = [Path(x) for x in dap_id_list]

	# bork if any of the directories do not exist
	for in_dir_path in in_dir_paths:
		if not in_dir_path.exists():
			logger.fatal(msg="Directory does not exist:" + in_dir_path.resolve().as_posix())
			sys.exit(1)

	# create the output directory if it does not exist
	out_dir = Path(config.root_dir).joinpath('rawdata')
	out_dir.mkdir(parents=True, exist_ok=True)

	#get the things to shortlist by
	backends = args.backends.split(",") if args.backends is not None else None
	sources = args.sources.split(",") if args.sources is not None else None
	frequencies = args.frequencies.split(",") if args.frequencies is not None else None 


	for in_dir_path, dap_id, pid, alt_name in zip(in_dir_paths, dap_id_list, pid_list, alt_name_list):

		dap_dir= in_dir_path.name

		file_list = []

		for ext in args.extensions.split(","):
			d = in_dir_path.resolve().as_posix()
			logger.debug("looking for " + d + "/*"+ ext)
			file_list.extend(glob.glob(d + "/*"+ ext)) 

		#Get file infos and sort by start time. 
		file_infos=get_file_infos(file_list, backends, sources, frequencies)
		file_infos.sort(key=lambda x: x.cfreq)
		logger.debug(file_infos)



		collection = Collection(collection_name = dap_dir, name_alias = alt_name, pid=pid, collection_path=in_dir_path.resolve().as_posix())

		file_info_groups = [list(g[1]) for g in groupby( file_infos, lambda o: o.cfreq)] 


		for file_info_group in file_info_groups:

			file_info_group.sort(key=lambda x: x.start_mjd)

			for file_info in file_info_group:
				logger.debug("considering {}".format(file_info.file_name))

				file_path = Path(file_info.file_name)
				file_ext = "".join(file_path.suffixes)


				dap_path = out_dir.joinpath(pid).joinpath(file_info.source).joinpath(dap_dir+"_"+alt_name)
				dap_path.mkdir(parents=True, exist_ok=True)

				utc_start = get_utc_string(file_info.start_mjd) 
				utc_end   = get_utc_string(file_info.end_mjd)

				utc_dir = utc_start


				times_file_path = dap_path.joinpath(TIMES_FILE).resolve()

				if times_file_path.exists():

					times_for_cfreq = get_times_for_cfreq(times_file_path, file_info.cfreq)

					#check if the file is not empty, meaning there are files at this frequency
					if times_for_cfreq.size is not 0:


						#if the files are the same - .rf vs .zrf, look if there are already UTCs with the same name
						if times_for_cfreq[ times_for_cfreq['utc_dir'] == utc_dir.encode() ].size is not 0:
							logger.info("Using existing utc_start=utc_dir for {}".format(utc_start))


						else:

							#current obs succeeds the previous ones as they are sorted, so mjd_start_now always > mjd_end_before, so this is always positive
							times_for_cfreq = sorted(times_for_cfreq,key=lambda x: (file_info.start_mjd-x['mjd_end']))

							#if there is a very close UTC
							if(file_info.start_mjd  - times_for_cfreq[0]['mjd_end'] < TOLERANCE):
								utc_close = times_for_cfreq[0]['utc_start']
								utc_dir = times_for_cfreq[0]['utc_dir'].decode() # change the utc directory to the start utc of the observation


				with open(times_file_path,'a') as f:
					line="{:8.3f} {:20.12f} {} {:20.12f} {} {}\n".format(file_info.cfreq, file_info.start_mjd, utc_start, file_info.end_mjd, utc_end,  utc_dir)
					logger.debug("writing to times.dat: {}".format(line))
					f.write(line)


				new_path = dap_path.joinpath(utc_dir).joinpath(str(file_info.cfreq))
				new_path.mkdir(parents=True, exist_ok=True)

				new_file_path = new_path.joinpath(utc_start + file_ext)
				if not new_file_path.exists():
					new_file_path.symlink_to(file_path)
					observation_chunk = file_info.observation_chunk
					observation_chunk.sym_file = new_file_path.absolute().as_posix()
					observation_chunk.original_file = file_path.resolve().as_posix()
					observation_chunk.obs_start_utc = utc_dir
					collection.observation_chunks.append(observation_chunk)
				else:
					logger.warn("{} already exists, skipping...".format(new_file_path.resolve().as_posix() ))    

			session.add(collection)
			


			# group observation chunks by start utc and add it to observations table
			for group in [list(g[1]) for g in groupby(collection.observation_chunks, lambda o: o.obs_start_utc)]:
				observation = None
				with session.no_autoflush:
					observation = db_manager.get_session().query(Observation).filter(
						Observation.obs_start_utc == group[0].obs_start_utc).filter(Observation.cfreq == group[0].cfreq).first()
					if observation is None:
						observation = Observation(group)
					else:
						observation.observation_chunks.extend(group)
					session.add(observation)
			session.commit()


	session.commit()
	print("All Done")



 
if __name__ == "__main__":
	main()
	

















