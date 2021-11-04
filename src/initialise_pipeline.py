from log import Logger
import argparse
from gen_utils import ensure_directory_exists, ensure_directory_is_not_empty
from pathlib import Path
from constants import PIPELINE_DIR_NAMES
from config_parser import ConfigurationReader
import traceback
import os 
from db_orms import DBManager
from exceptions import IncorrectInputsException
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

    # open an empty database
    db_path = out_path.joinpath("psrpype.sqlite3")

    if db_path.exists():
        raise IncorrectInputsException("DB already exists here: {} ".format(out_path.resolve().as_posix()))

    for d in PIPELINE_DIR_NAMES:
        out_path.joinpath(d).mkdir(parents=True, exist_ok=True) 

    # write a default config file
    ConfigurationReader.init_default(out_path)

    db_manager = DBManager.get_instance(db_path)

    db_manager.init_database()



   #session = db_manager.start_session()




    logger.info("Pipeline initialised in {}".format(out_path.resolve().as_posix()))

