import argparse
import glob
import logging
import os
import os.path
from log import *
import numpy as np
from utils import *
from integrator import Integrator



def get_args():
    argparser = argparse.ArgumentParser(description="Run PSRPYPE pipeline")
    argparser.add_argument("-config_file", dest="config_file", help="config_file", default="uwl.config")
    argparser.add_argument("-s", "-sources", dest="sources", help="comma separated names of sources", default="all")
    argparser.add_argument("-verbose", dest="verbose", help="Enable verbose terminal logging", action="store_true")
    argparser.add_argument("-log_file", dest="log_file", help="Name of log file",default="psrpype.log")
    argparser.add_argument("-slurm", dest="slurm", help="Submit as a slurm job", action="store_true" )

    args = argparser.parse_args()
    return args


if __name__ == "__main__":


    # initialise arg parsing
    args = get_args()

    # Inititalise logging

    logger = init_logging(file_name=args.log_file)



    # initialise config read
    if args.config_file is not None and os.path.isfile(args.config_file):
        config = parse_config(args.config_file)
    else:
        print("Config file not provided or incorrect. Proceeding with default")
        config = parse_config()


    






    

    

    # integrator.init_dirs_to_process()
    # integrator.init_files_to_process()

    '''
    with open(os.path.join(output_dir,str(output_file)),'w') as script_file:
        script_file.write("#!/bin/bash \n")
        script_file.write("#SBATCH --job-name={0}_{1} \n".format(psrnames[obs_num],obs_num))
        script_file.write("#SBATCH --output={0}meerpipe_out_{1}_{2} \n".format(str(output_dir),psrnames[obs_num],obs_num))
        script_file.write("#SBATCH --ntasks=1 \n")
        script_file.write("#SBATCH --mem=160g \n")
        script_file.write("#SBATCH --time=10:00:00 \n")
        script_file.write("python slurm_pipe.py -obsname {0}archivelist.npy -outputdir {0}output.npy -cfile {1}".format(output_dir,str(args.configfile)))

    '''
