# Psrpype V1.0

This is a pipeline that can process fold-mode data from the Parkes UWL receiver, taken with Medusa, CASPSR and DFB4 backends. 

This can be used both standalone and via slurm. 

## Installation

Clone this repository by doing `git clone https://github.com/vivekvenkris/psrpype.git`

The pipeline requires python 3 to work. The required python libraries are provided in `requirements.txt`. You can install all of them by doing `pip install -r requirements.txt`. If you cannot install your libraries globally due to lack of permissions, you can do `pip install --user -r requirements.txt` to install them in your user directory. 

Other than this, you also need a working version of `psrchive` in your `$PATH`.


## Usage

1. set the PSRPYPE environment to the root directory of the repository. For example: `export PSRPYPE="/fred/oz002/vvenkatr/psrpype`
2. Initialise the pipeline: `python $PSRPYPE/src/initialise_pipeline.py -d /your/path/to/psrpype_out`
    This will initialise the pipeline output directory with the required files and folders. This will also copy any resources (i.e. flux cal, metm files) from the repository and obtain the corresponding calibration solutions. 
    At this time, you can change any of the config options by editing `/your/path/to/psrpype_out/default.cfg`
3. Initialise the data you are trying to process: `python $PSRPYPE/src/initialise_data.py --config=/your/path/to/psrpype_out/default.cfg --dir_list=/path/to/dir.list --sources="JXXXX-XXXX,JTTTT-TTTT" --freqs="2368.0,1382" --backends="Medusa,CASPSR"`
    This will go through all the files, shortlist the observations that you want and then add them to the database for further processing. 
    the dir.list is a file that contains three columns that are  PID, ABSOLUTE_DIR_PATH and ALT_NAME where PID is the project ID, ABSOLUTE_DIR_PATH is the path to the directory containing the files that you want to process. This is usually the directory you download from the data access portal (DAP).  ALT_NAME is an alternate name you can give to it to easily identify the data. Eg: 2018APRS_02
    Instead of `--dir_list` you can also provide `-d` with the same information but in a comma separated format in the command line. For example: `-d "P971 /path/to/dap/dir 2018APRS_01,P971 /path/to/dap/dir 2018APRS_02"

4. Prepare the calibration files: `python  $PSRPYPE/src/prepare_cals.py --config=/your/path/to/psrpype_out/default.cfg`
5. Process data: `python $PSRPYPE/src/prepare_data.py --config=/your/path/to/psrpype_out/default.cfg --with_slurm`. This will process all the pulsar data in the DB, with slurm (you can ignore this option if you want to run everything sequentially on the same machine). You can shortlist what you want to process with the command line options similar to Step 2. 

Please do `-h` to obtain the arguments that each of the above programs accept. 


