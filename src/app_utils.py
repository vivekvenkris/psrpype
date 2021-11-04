from gen_utils import split_and_strip
from db_orms import *

class AppUtils(object):

	@staticmethod
	def add_shortlist_options(argparser):
		shortlist_group = argparser.add_argument_group('Shortlist options')

		shortlist_group.add_argument("-b", "--backends", dest="backends", help="comma separated backends list to process, (default: ALL)")
		shortlist_group.add_argument("-s", "--sources", dest="sources", help="comma separated sources list to process, (default: ALL)")
		shortlist_group.add_argument("-c", "--centre_frequencies", dest="frequencies", help="comma separated centre frequencies list to process, (default: ALL)")
		shortlist_group.add_argument("-o", "--obs_utcs", dest="obs_utcs", help="comma separated UTC list to process, (default: ALL)")

	@staticmethod
	def add_shortlist_filters(query, args):

		if args.backends is not None:
			query = query.filter(Observation.backend.in_(split_and_strip(args.backends, ",")))

		if args.sources is not None:
			query = query.filter(Observation.source.in_(split_and_strip(args.sources, ",")))

		if args.frequencies is not None:
			query = query.filter(Observation.cfreq.in_(split_and_strip(args.frequencies, ",")))

		if args.obs_utcs is not None:
			query = query.filter(Observation.obs_start_utc.in_(split_and_strip(args.obs_utcs, ",")))

		return query

