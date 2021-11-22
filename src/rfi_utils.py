from db_orms import *
import numpy as np
import psrchive as ps
from threading import Thread



class Cleaner(object):

	def __init__(self, config, type, tolerance=1):
		self.type = type
		self.tolerance = tolerance
		self.config = config
		self.logger = Logger.get_instance()

	def clfd_cleaner(self, observation_chunk, in_file, cleaned_file_path):

		import clfd
		from clfd.interfaces import PsrchiveInterface

		dirty_channels = self._get_known_dirty_channels(observation_chunk, self.tolerance)
		self.logger.info("{} channels are dirty...".format(len(dirty_channels)))

		archive = ps.Archive_load(in_file)

		cube = clfd.DataCube(archive.get_data()[:, 0, :, :]) 
		features = clfd.featurize(cube, features=('std', 'ptp', 'lfamp','skew', 'kurtosis', 'acf'))
		stats, mask = clfd.profile_mask(features, q=4.0,  zap_channels=dirty_channels)	
	
		PsrchiveInterface.apply_profile_mask(mask, archive)

		archive.unload(cleaned_file_path.resolve().as_posix())

		del archive, mask, cube

	
	def coast_guard_cleaner(self, observation, in_file, cleaned_file_path):
		pass




	def clean_and_save(self, observation_chunk, out_dir):


		cleaned_file_path = observation_chunk.construct_output_archive_path(self.config.root_dir_path, out_dir)
		in_file = None

		if out_dir is "cleaned":
			in_file = observation_chunk.preprocessed_file  if observation_chunk.preprocessed_file is not None else observation_chunk.sym_file
		else:
			in_file = observation_chunk.calibrated_file


		if cleaned_file_path.exists():
			self.logger.warn("Cleaned file already exists: {}, skipping...".format(cleaned_file_path))
		else: 
			if not Path(in_file).exists():
				self.logger.fatal("Input file: {} does not exist".format(in_file))
				return

			if self.type == "clfd":
				self.clfd_cleaner(observation_chunk, in_file, cleaned_file_path)
			elif self.type == "coast_guard":
				self.coast_guard_cleaner(observation_chunk, in_file, cleaned_file_path)
			else:
				self.logger.fatal("Unknown cleaner type: {}".format(self.type))

		if out_dir is "cleaned":
			observation_chunk.cleaned_file = cleaned_file_path.resolve().as_posix() 
		else:
			observation_chunk.recleaned_file = cleaned_file_path.resolve().as_posix() 

		



	def _get_known_rfi(self, backend, tolerance):

		# List of known RFI channels (low, high in MHz) obtained from George Hobbs' RFI database

		rfi_list = []

		if backend == "Medusa":

			band_edge = 10; # MHz
			lowest_freq = 704 # MHz
			sub_band_bw = 128 # MHz
			num_sub_bands = 32 

			# subband edges
			for i in range(num_sub_bands):
				rfi_list.append((lowest_freq + sub_band_bw * i - band_edge/2., lowest_freq + sub_band_bw * i + band_edge/2.))

			# Transmission towers
			rfi_list.append((758.0,768.0))  # Optus
			rfi_list.append((768.0,788.0))  # Telstra
			rfi_list.append((870.0,875.0))  # Vodafone
			rfi_list.append((875.0,890.0))  # Telstra
			rfi_list.append((915.0,928.0))  # Agricultural 920 MHz RFI
			rfi_list.append((943.4,951.8))  # Optus
			rfi_list.append((953.7,958.7))  # Vodafone
			rfi_list.append((1081.7,1086.7))  # Aliased signal
			rfi_list.append((1720.0,1736.2))  # Aliased signal
			rfi_list.append((1805.0,1825.0))  # Telstra
			rfi_list.append((1845.0,1865.0))  # Optus
			rfi_list.append((1973.7,1991.0))  # Aliased signal
			rfi_list.append((2110.0,2120.0))  # Vodafone
			rfi_list.append((2140.0,2145.0))  # Optus
			rfi_list.append((2145.0,2150.0))  # Optus
			rfi_list.append((2164.9,2170.1))  # Vodafone
			rfi_list.append((2302.1,2321.9))  # NBN
			rfi_list.append((2322.1,2341.9))  # NBN
			rfi_list.append((2342.1,2361.9))  # NBN
			rfi_list.append((2362.1,2381.9))  # NBN
			rfi_list.append((2487.0,2496.0))  # NBN alias
			rfi_list.append((2670.0,2690.0))  # Optus
			rfi_list.append((3445.1,3464.9))  # NBN
			rfi_list.append((3550.1,3569.9))  # NBN

			#other narrow band transmission

			rfi_list.append((804.4,804.6))  # NSW Police Force
			rfi_list.append((2152.5 - 2.50,2152.5 + 2.50))  # Telstra - not always on
			rfi_list.append((847.8  - 0.20,847.8  + 0.20))  # Radio broadcast Parkes
			rfi_list.append((849.5  - 0.10,849.5  + 0.10))  # NSW Police Force
			rfi_list.append((848.6  - 0.12,848.6  + 0.12))  # Radio broadcast Mount Coonambro
			rfi_list.append((2127.5 - 2.50,2127.5 + 2.50))  # Parkes: Station open to official correspondence exclusively
			
			if(tolerance > 1):
				rfi_list.append((3575.0,3640.0))  # Telstra from Orange or Dubbo


			rfi_list.append((1023.0,1025.0))  # digitizer related signal
			rfi_list.append((1919.9,1920.1))  # digitizer related signal
			rfi_list.append((3071.9,3072.1))  # digitizer related signal

			rfi_list.append((825.0,825.0))  # unexplained
			rfi_list.append((1225.0,1230.0))  # unexplained
			rfi_list.append((1399.9,1400.2))  # unexplained
			rfi_list.append((1498.0,1499.0))  # unexplained
			rfi_list.append((1499.8,1500.2))  # unexplained
			rfi_list.append((1880.0,1904.0))  # unexplained
			rfi_list.append((2032.0,2033.0))  # unexplained
			rfi_list.append((2063.0,2064.0))  # unexplained
			rfi_list.append((2077.0,2078.0))  # unexplained
			rfi_list.append((2079.0,2080.0))  # unexplained
			rfi_list.append((2093.0,2094.0))  # unexplained
			rfi_list.append((2160.0,2161.0))  # unexplained
			rfi_list.append((2191.0,2192.0))  # unexplained
			rfi_list.append((2205.0,2206.0))  # unexplained
			rfi_list.append((2207.0,2208.0))  # unexplained
			rfi_list.append((2221.0,2222.0))  # unexplained
			rfi_list.append((2226.3,2226.7))  # unexplained
			rfi_list.append((1618.0,1626.5))  # Iridium
			
			if (tolerance > 1):
				rfi_list.append((1164.0,1189.0))  # satellite
				rfi_list.append((1189.0,1214.0))  # satellite
				rfi_list.append((1240.0,1260.0))  # satellite
				rfi_list.append((1260.0,1300.0))  # satellite

			if (tolerance > 2):
				rfi_list.append((1525.0,1646.5))  # Inmarsat
				rfi_list.append((2401.0,2483.0))  # Entire wifi band


			rfi_list.append((703.0,713.0)) # 4G Optus
			rfi_list.append((704.5,708.0)) # Alias
			rfi_list.append((713.0,733.0))
			rfi_list.append((825.1,829.9)) # Vodafone 4G
			rfi_list.append((830.0,845.0))  # Telstra 3G
			rfi_list.append((847.6,848.0))
			rfi_list.append((898.4,906.4))# Optus 3G
			rfi_list.append((906.8,915.0)) # Vodafone 3G
			rfi_list.append((953.0,960.1))    # Alias
			rfi_list.append((1710.0,1725.0))  # 4G Telstra
			rfi_list.append((1745.0,1755.0))  # 4G Optus
			rfi_list.append((2550.0,2570.0))  # 4G Optus
			rfi_list.append((1017.0,1019.0))  # Parkes ground response
			rfi_list.append((1029.0,1031.0))  # Parkes ground response
			rfi_list.append((1026.8,1027.2))  # Unknown DME signal
			rfi_list.append((1027.8,1028.2))  # Unknown DME signal
			rfi_list.append((1032.8,1033.2))  # Unknown DME signal
			rfi_list.append((1040.8,1041.2))  # Strong, unknown DME signal
			rfi_list.append((1061.8,1062.2))  # Sydney DME
			rfi_list.append((1067.8,1068.2))  # Richmond DME
			rfi_list.append((1071.8,1072.2))  # Wagga Wagga DME
			rfi_list.append((1079.2,1080.2))  # Unknown DME
			rfi_list.append((1080.8,1081.2))  # Parkes DME
			rfi_list.append((1081.8,1082.2))  # Sydney RW07 DME
			rfi_list.append((1103.8,1104.2))  # Unknown DME
			rfi_list.append((1102.8,1103.2))  # Unknown DME
			rfi_list.append((1120.8,1121.2))  # Wagga Wagga DME
			rfi_list.append((1134.8,1135.2))  # Nowra DME
			rfi_list.append((1137.6,1138.4))  # Canberra DME
			rfi_list.append((1149.8,1150.2))  # Likely DME
			rfi_list.append((1150.8,1151.2))  # Likely DME

		return rfi_list

	def _get_known_dirty_channels(self, observation, tolerance):

		backend = observation.backend

		rfi_list = self._get_known_rfi(backend, tolerance)

		nchan = observation.nchan
		bw = observation.bw
		chan_bw = bw/nchan

		lowest_freq = observation.cfreq - observation.bw/2.

		dirty_channels = np.array([])

		for band in rfi_list:
			low_ichan = np.floor((band[0] - lowest_freq)/chan_bw) 
			high_ichan = np.ceil((band[1] - lowest_freq)/chan_bw)

			dirty_channels = np.concatenate((dirty_channels, np.arange(low_ichan if low_ichan >= 0 else 0 , high_ichan if high_ichan < nchan -1 else nchan -1)))


		return np.unique(dirty_channels).astype(int)























	