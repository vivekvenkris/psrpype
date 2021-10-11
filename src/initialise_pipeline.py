from log import Logger
import argparse
from gen_utils import ensure_directory_exists, ensure_directory_is_not_empty
from pathlib import Path
from constants import PIPELINE_DIR_NAMES
from config_parser import ConfigurationReader
import traceback
import os 
from db_orms import DBManager
def get_args():
    argparser = argparse.ArgumentParser(description="Initialise pipeline")
    argparser.add_argument("-d", "--pipeline_root_dir", dest="pipeline_root_dir", help="Directory to place processed output", required=True)
    Logger.add_logger_argparse_options(argparser)

    args = argparser.parse_args()
    return args


if __name__ == "__main__":

    # initialise arg parsing
    args = get_args()

    #initialise logger
    logger = Logger.getInstance(args)

    #make the output directory
    out_path=Path(args.pipeline_root_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # write a default config file
    ConfigurationReader.init_default(out_path)

    # open an empty database
    db_path = out_path.joinpath("psrpype.db")

    if db_path.exists():
        raise IncorrectInputsException("{} Ledger already exists here: ".format(out_path.resolve().decode()))

    for d in PIPELINE_DIR_NAMES:
        Path().joinpath(d).mkdir(parents=True, exist_ok=True) 

    DBManager.init_db(db_path)


    logger.info("done")