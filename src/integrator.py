from defs import *
import numpy as np
import os

class Integrator(): 

    def __init__(self, config, source):

        self.config = config
        self.dirs_to_process = None
        self.files_to_process = None

    def init_dirs_to_process(self):

        input_dir = str(self.config.get(INPUT, INPUT_ROOT))
        input_sub_dirs = np.array(self.config.get(INPUT, DIRS).strip(' ').split(","))
        input_sub_dirs = np.array([x.strip(' ') for x in input_sub_dirs])

        dirs_to_check = []

        if input_sub_dirs is None or input_sub_dirs.size == 0:
            dirs_to_check.append(input_dir)

        else:

            for input_sub_dir in input_sub_dirs:
                sub_dir = os.path.join(input_dir, input_sub_dir)
                dirs_to_check.append(sub_dir)

        self.dirs_to_process = []

        for inp in dirs_to_check:
            source_dir = os.path.join(inp, args.source)
            if os.path.exists(source_dir):
                print("adding", source_dir)
                self.dirs_to_process.append(source_dir)

        self.dirs_to_process = np.array(self.dirs_to_process)

        print( self.dirs_to_proces)

    def init_files_to_process(self):

        extensions = str(config.get(INPUT, EXT)).strip(' ').split(",")
        extensions = np.array([x.strip(' ') for x in extensions])

        files_to_process = []

        for direc in self.dirs_to_process:

            for ext in extensions:
                files = sorted(glob.glob(os.path.join(direc, "*" + ext)))

                files_to_process.extend(files)

            for i in files_to_process:
                 print(os.path.getsize(i))
