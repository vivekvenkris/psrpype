import os
import psrchive as ps
from log import *
import logging
import argparse
import glob
from utils import *

def get_args():
    argparser = argparse.ArgumentParser(description="Get information about archive files in the directory")

    group = argparser.add_mutually_exclusive_group(required=True)
    group.add_argument("-d", "--dir", dest="dir", help="directory to get files from")
    group.add_argument("-f", "--files", dest="file_list", help="list of files", nargs='+')

    argparser.add_argument("-e", "--extensions", dest="extensions", help="comma separated extensions of archive files to use", default=".ar,.cf,.rf,.zcf,.zrf")
    argparser.add_argument("-i", "--info_file_name", dest="info_file_name", help="Name of the info file generated", default="dir")
    argparser.add_argument("-u", "--unload_directory", dest="out_dir", help="directory to unload the info file", default="dir")
    argparser.add_argument("-v", "-verbose", dest="verbose", help="Enable verbose terminal logging", action="store_true")

    args = argparser.parse_args()
    return args

 
if __name__ == "__main__":


	level = logging.WARN

	args = get_args()
	if(args.verbose):
		level = logging.DEBUG

	logger = init_logging(file_name='get_info.log', file_level=level)

	if not os.path.exists(args.dir):
		logger.fatal(msg="Invalid directory:" + args.dir) 





	
	args.info_file_name = os.path.basename(os.path.abspath(args.dir)) + ".info" if "dir" == args.info_file_name else args.info_file_name
	args.out_dir = args.dir if "dir" == args.out_dir else args.out_dir

	outfile = os.path.join(args.out_dir, args.info_file_name)


	backup_if_exists(outfile, logger)



	file_list = []

	for ext in args.extensions.split(","):

		file_list.extend(glob.glob(args.dir + "/*"+ext)) 


	infos = []

	info=[]
	info.append("#rawfile")
	info.append("source")
	info.append("start_mjd")
	info.append("nchan")
	info.append("nsubint")
	info.append("npol")
	info.append("cfreq")
	info.append("bw")
	info.append("backend")
	info.append("telescope")
	info.append("type")
	info.append("file_size")

	infos.append(info)



	for file in file_list:

		ar = ps.Archive_load(file)

		info=[]
		info.append(os.path.abspath(file))
		info.append(ar.get_source())
		info.append(ar.start_time().printall())
		info.append(str(ar.get_nchan()))
		info.append(str(ar.get_nsubint()))
		info.append(str(ar.get_npol()))
		info.append(str(ar.get_centre_frequency()))
		info.append(str(ar.get_bandwidth()))
		info.append(ar.get_backend_name())
		info.append(ar.get_telescope())
		info.append(ar.get_type())

		info.append(str(os.path.getsize(file)/1e9))

		infos.append(info) 


 



	widths = [max(map(len, col)) for col in zip(*infos)]

	text_to_save= ""

	for row in infos:
		text_to_save = text_to_save + "  ".join((val.ljust(width) for val, width in zip(row, widths))) + " \n"

	logger.info("Writing to " + os.path.join(args.out_dir, args.info_file_name))

	with  open(outfile, 'w') as f:
		f.write(text_to_save)

	logger.info("done")













