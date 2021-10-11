import glob, argparse, datetime, sys
import numpy as np
from pathlib import Path, PurePath
import psrchive as ps
from astropy.time import Time
from log import Logger
import numpy.lib.recfunctions as rfn

TIMES_FILE="times.dat"
TOLERANCE=0.00010 # < 10 seconds in MJD


def get_args():
	argparser = argparse.ArgumentParser(description="Generate usual directory structure by symlinking files downloaded from DAP", 
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	required_group = argparser.add_argument_group('Required arguments')
	required_group.add_argument("-o", "--out_directory", dest="out_dir", help="output directory", required=True)
	required_group.add_argument("-p", "--pid", dest="pid", help="Project ID", required=True)


	mutually_exclusive = argparser.add_argument_group('Mutually exclusive arguments')

	group = mutually_exclusive.add_mutually_exclusive_group(required=True)
	group.add_argument("-d", "--dirs", dest="dirs", help="directories to get files from", nargs='+', default=argparse.SUPPRESS)
	group.add_argument("-f", "--files", dest="file_list", help="list of files", nargs='+', default=argparse.SUPPRESS)

	argparser.add_argument("-e", "--extensions", dest="extensions", help="comma separated extensions of archive files to use", default=".ar,.cf,.rf,.zcf,.zrf")
	argparser.add_argument("-b", "--backends", dest="backends", help="comma separated backends list to process, (default: ALL)")
	argparser.add_argument("-s", "--sources", dest="sources", help="comma separated sources list to process, (default: ALL)")
	argparser.add_argument("-c", "--centre_frequencies", dest="frequencies", help="comma separated centre frequencies list to process, (default: ALL)")

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

	def __repr__(self):
		return self.__str__()

	def __str__(self):
		return "{} {} {} {} {} {} \n".format(self.file_name, self.backend, self.source, self.cfreq, self.start_mjd, self.end_mjd)


def get_file_infos(file_list, backends, sources, frequencies):
	logger = Logger.getInstance()
	file_infos = []
	for file in file_list:
		ar = ps.Archive_load(file)

		if(backends is not None and ar.get_backend_name() not in backends):
			logger.debug("skipping " + file + " as not in backends list")
			continue

		if(sources is not None and ar.get_source_name() not in sources):
			logger.debug("skipping " + file + " as not in sources list")
			continue

		if(frequencies is not None and ar.get_centre_frequency() not in frequencies):
			logger.debug("skipping " + file + " as not in cenre frequencies list")
			continue	

		file_info = FileInfo(file, ar)
		file_infos.append(file_info)
		del ar

	return file_infos



def get_times_for_cfreq(file_name, cfreq):
	times = np.loadtxt(file_name, dtype={'names': ('cfreq', 'mjd_start', 'utc_start', 'mjd_end', 'utc_end', 'utc_dir'),
	                     'formats': (np.float, np.float, 'S32',  np.float, 'S32', 'S32')}) # UTC_START CFREQ MJD_END
	times_for_cfreq = times[ times['cfreq'] == cfreq ] 
	return times_for_cfreq


def main():
	np.set_printoptions(precision=8)

	args = get_args()
	logger = Logger.getInstance(args)

	directories = args.dirs if args.dirs is not None else ['standalone']
	in_dir_paths = [Path(x) for x in directories]

	# bork if any of the directories do not exist, ignore standalone processing
	for in_dir_path in in_dir_paths:
		if not in_dir_path.exists() and in_dir_path.name is not 'standalone':
			logger.fatal(msg="Directory does not exist:" + args.dir)
			sys.exit(1)

	# create the output directory if it does not exist
	out_dir = Path(args.out_dir)
	out_dir.mkdir(exist_ok=True)

	backends = args.backends.split(",") if args.backends is not None else None
	sources = args.sources.split(",") if args.sources is not None else None
	frequencies = args.frequencies.split(",") if args.frequencies is not None else None 


	for in_dir_path in in_dir_paths:

		dap_dir= in_dir_path.name

		file_list = []


		if in_dir_path.name is not "standalone": 

			for ext in args.extensions.split(","):
				d = bytes(in_dir_path.resolve()).decode()
				logger.debug("looking for " + d + "/*"+ ext)
				file_list.extend(glob.glob(d + "/*"+ ext)) 

		else:
			file_list = args.file_list

		#Get file infos and sort by start time. 
		file_infos=get_file_infos(file_list, backends, sources, frequencies)
		file_infos.sort(key=lambda x: x.start_mjd)
		logger.debug(file_infos)



		for file_info in file_infos:
			logger.debug("considering", file_info.file_name)

			file_path = Path(file_info.file_name)
			file_ext = "".join(file_path.suffixes)


			dap_path = out_dir.joinpath(args.pid).joinpath(file_info.source).joinpath(dap_dir)
			dap_path.mkdir(parents=True, exist_ok=True)

			utc_start = Time(file_info.start_mjd,format="mjd", scale="utc").isot.replace("T","-").split(".")[0]
			utc_end   = Time(file_info.end_mjd  ,format="mjd", scale="utc").isot.replace("T","-").split(".")[0]

			utc_dir = utc_start


			times_file_path = dap_path.joinpath(TIMES_FILE).resolve()

			if times_file_path.exists():

				times_for_cfreq = get_times_for_cfreq(times_file_path, file_info.cfreq)

				#check if the file is not empty, meaning there are files at this frequency
				if times_for_cfreq.size is not 0:


					#if the files are the same - .rf vs .zrf, look if there are already UTCs with the same name
					if times_for_cfreq[ times_for_cfreq['utc_dir'] == utc_dir.encode() ].size is not 0:
						logger.info("Using existing utc_start=utc_dir for" + utc_start)


					else:

						#current obs succeeds the previous ones as they are sorted, so mjd_start_now always > mjd_end_before, so this is always positive
						times_for_cfreq = sorted(times_for_cfreq,key=lambda x: (file_info.start_mjd-x['mjd_end']))

						#if there is a very close UTC
						if(file_info.start_mjd  - times_for_cfreq[0]['mjd_end'] < TOLERANCE):
							utc_close = times_for_cfreq[0]['utc_start']
							utc_dir = times_for_cfreq[0]['utc_dir'].decode() # change the utc directory to the start utc of the observation


			with open(times_file_path,'a') as f:
				line="{:8.3f} {:20.12f} {} {:20.12f} {} {}\n".format(file_info.cfreq, file_info.start_mjd, utc_start, file_info.end_mjd, utc_end,  utc_dir)
				logger.debug("writing to times.dat: ", line)
				f.write(line)


			new_path = dap_path.joinpath(utc_dir).joinpath(str(file_info.cfreq))
			new_path.mkdir(parents=True, exist_ok=True)

			new_file_path = new_path.joinpath(utc_start + file_ext)
			if not new_file_path.exists():
				new_file_path.symlink_to(file_path)
			else:
				logger.warn(bytes(new_file_path).decode() + "already exists, skipping...")    


 
if __name__ == "__main__":
	main()
	

















