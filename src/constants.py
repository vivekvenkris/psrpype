

TEMPLATE_DIR="templates"
EPHEMERIS_DIR="ephemeris"
FLUXCAL_DIR="flux_cal"
POLNCAL_DIR="poln_cal"
PULSAR_DIR="pulsar"
SCRATCH_DIR="scratch"
PIPELINE_DIR_NAMES=[TEMPLATE_DIR, EPHEMERIS_DIR, FLUXCAL_DIR, POLNCAL_DIR, PULSAR_DIR, SCRATCH_DIR]

FLUXCAL_CLEANED_DIR = "cleaned"
POLNCAL_CLEANED_DIR = "cleaned"

FLUXCAL_SOLUTIONS_DIR = FLUXCAL_DIR + "/solutions"
POLNCAL_SOLUTIONS_DIR = POLNCAL_DIR + "/solutions"
FLUXCAL_DIRS=[FLUXCAL_CLEANED_DIR, FLUXCAL_SOLUTIONS_DIR]
POLNCAL_DIRS=[POLNCAL_CLEANED_DIR, POLNCAL_SOLUTIONS_DIR]

PULSAR_CLEANED_PRE_CAL = "cleaned_pre-cal"
PULSAR_CALIBRATED = "calibrated"
PULSAR_CLEANED_POST_CAL = "cleaned_post-cal"
PULSAR_DECIMATED = "decimated"
PULSAR_TIMING = "timing"



PULSAR_DIRS=[PULSAR_CLEANED_PRE_CAL, PULSAR_CALIBRATED, PULSAR_CLEANED_POST_CAL, PULSAR_DECIMATED, PULSAR_TIMING]


FLUX_CALIBRATOR_SOURCES=["0407-658_N", "0407-658_O", "0407-658_S", "0823-500_NS", "0823-500_SN", 
"1934-638_N", "1934-638_O", "1934-638_S", "B0407-658_N", "B0407-658_O", "B0407-658_S", 
"HYDRA_N", "HYDRA_O", "HYDRA_S", "HydraA_N", "HydraA_O", "HydraA_S"]

CALIBRATOR_TYPES=['PolnCal', 'FluxCal-On', 'FluxCal-Off']
PULSAR_TYPES=['Pulsar']


