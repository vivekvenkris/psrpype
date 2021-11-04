import sys
import os
import subprocess
import hashlib
from datetime import datetime
from gen_utils import run_process
from pathlib import Path
from log import Logger

TEMPLATE = """\
#!/bin/bash

#SBATCH -e {log_dir}/{name}.%J.err
#SBATCH -o {log_dir}/{name}.%J.out
#SBATCH -J {name}

#SBATCH --mail-user={mail_user}
#SBATCH --mail-type={mail_type}
#SBATCH --partition={partition}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --ntasks={ntasks}
#SBATCH --time={time}
#SBATCH --account={account}

{bash_setup}

__script__"""


class Slurm(object):


    def __init__(self, config, template=TEMPLATE,  slurm_cmd="sbatch --export=ALL"):

        self.config = config
        self.slurm_config = config.slurm_config
        self.log_dir = config.root_dir_path.joinpath("slurm-logs").resolve().as_posix()
        self.scripts_dir = config.root_dir_path.joinpath("slurm-scripts").resolve().as_posix()
        self.logger = Logger.getInstance()

        config.root_dir_path.joinpath("slurm-logs").mkdir(exist_ok=True)
        config.root_dir_path.joinpath("slurm-scripts").mkdir(exist_ok=True)

        self.template = template
        self.slurm_cmd = slurm_cmd


    def run(self, source, obs_start_utc,process_cmd, memory, ncpus, wall_time):
        name = source + "_" + obs_start_utc + ".bash"

        script = self.template.format(name=name,
                                        log_dir=self.log_dir,
                                        bash_setup=self.config.slurm_config.bash_setup,
                                        mail_user=self.config.slurm_config.mail_user,
                                        mail_type= self.config.slurm_config.mail_type,
                                        partition = self.config.slurm_config.partition,
                                        mem_per_cpu = memory,
                                        ntasks=ncpus,
                                        time=wall_time,
                                        account="oz005"
                                ).replace("__script__", process_cmd)

        script_file = Path(self.scripts_dir).joinpath(name).resolve().as_posix()

        with open(script_file , "w") as sh:
            sh.write(script)

        command = "{} {}".format(self.slurm_cmd, script_file)

        output = run_process(command)

        job_id = None

        for line in output.splitlines():
            if "Submitted batch job" in line:
                job_id = line.split()[-1]

        if job_id is None:
            logger.fatal("job did not launch properly {}".format(name))

        return job_id

