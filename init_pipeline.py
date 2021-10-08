from log import Logger
import argparse
from utils import ensure_directory_exists, ensure_directory_is_not_empty
import os
from constants import PIPELINE_DIR_NAMES

def get_args():
    argparser = argparse.ArgumentParser(description="Initialise pipeline")
    argparser.add_argument("-pipeline_dir", dest="out_dir", help="Directory to place processed output", required=True)
    argparser.add_argument("-verbose", dest="verbose", help="Enable verbose terminal logging", action="store_true")

    args = argparser.parse_args()
    return args


if __name__ == "__main__":

    #initialise logger
    logger = Logger(file_name='add_to_ledger.log').logger


    # initialise arg parsing
    args = get_args()


    if not os.path.exists(args.out_dir):
		logger.info("creating directory:" + args.out_dir + " as it does not exist")
		os.mkdir(args.out_dir)

    ledger_file = os.path.join(args.out_dir,"ledger.txt")

    if os.path.exists(ledger_file):
    	logger.fatal(ledger_file + " already exists. Maybe you have initialised the pipeline already?")
    	exit(1)


    for directory in PIPELINE_DIR_NAMES:

    	p = os.path.join(args.out_dir, directory)
    	if(not os.path.exists(p)):
			os.mkdir(p)


    open(ledger_file, 'a').close()

    logger.info("done")