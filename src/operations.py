import os
import psrchive as ps
from log import *
import logging
import argparse
import glob
from coast_guard import cleaners
from utils import *




def run_coast_guard(ar,threshold_time=7, threshold_freq=7, template=None):

	logger.info("Applying the surgical cleaner")

	surgical_cleaner = cleaners.load_cleaner('surgical')
	surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={0},subintthresh={1}'.format(threshold_freq,threshold_time)

	if(template is not None):
		logger.info("Not using a template")
		surgical_parameters = surgical_parameters + ',template={0}'.format(template)

	surgical_cleaner.parse_config_string(surgical_parameters)
	surgical_cleaner.run(loaded_archive)


	logger.info("Applying rcvrstd cleaner")
    rcvrstd_cleaner = cleaners.load_cleaner('rcvrstd')
    rcvrstd_parameters = 'badfreqs=None,badsubints=None,trimbw=0,trimfrac=0,trimnum=0,response=None' # add default RFI flag
    rcvrstd_cleaner.parse_config_string(rcvrstd_parameters)
    rcvrstd_cleaner.run(loaded_archive)

    logger.info("Applying the bandwagon cleaner")
    bandwagon_cleaner = cleaners.load_cleaner('bandwagon')
    bandwagon_parameters = 'badchantol=0.99,badsubtol=1.0'
    bandwagon_cleaner.parse_config_string(bandwagon_parameters)
    bandwagon_cleaner.run(loaded_archive)

	return ar



def flatten_bandpass(ar):
    for ss in xrange(nsubint):
            for ff in xrange(nchan):
                    weight = loaded_archive.get_Integration(ss).get_weight(ff)
                    for pp in xrange(npol):
                            logger.debug("(%d, %d, %d) out of (%d, %d, %d)" % (pp, ss, ff, npol, nsubint, nchan))
                            prof = loaded_archive.get_Integration(ss).get_Profile(pp,ff)
                            if weight == 0.0:
                                    loaded_archive.get_Integration(ss).get_Profile(pp,ff).scale(0)
                                    continue
                            osm, osr = sc.probplot(prof.get_amps(), sparams=(), dist='norm', fit=0)
                            q_max = np.min(np.where(osm > 1.0))
                            q_min = np.max(np.where(osm < -1.0))
                            rms, mean = np.polyfit(osm[q_min:q_max], osr[q_min:q_max], 1)
                            prof.offset(-mean)
                            if rms == 0.0: prof.scale(0)
                            else: prof.scale(1./rms)
     return ar



def flux_calibrate(ar):
	print "Hello"


def poln_calibrate(ar):
	print "Hello"




def generate_flux_cal_database(metafile, logger, outpath):

	pac_command = "pac -W {0}".format(metafile)
	run_subprocess(generate_flux_cal_database.__name__, pac_command, logger)

	ensure_directory_exists(outpath)
	ensure_file_exists("database.txt")

	move_file(get_path(".", "database.txt"), get_path(outpath, "database.txt"))

	




	fluxcal_command = "fluxcal -d"


def generate_poln_cal_database():
	print "Hello"





































