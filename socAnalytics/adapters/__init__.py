import adapters.fb.fanpage
import sys

# helper class for proper initialization of an adapter
class Helper:

	# contructor
	def __init__(self, conf, currentDb):
		self.db = currentDb
		self.conf = conf


	# returns adapter based on config
	def GetAdapter(self):
		adapters = {
			"fbFanpage" : self._initializeFbFanpage,
			}

		if self.conf.adapter["type"] in adapters.keys():
			return adapters[self.conf.adapter["type"]]()
		else:
			print("No adapter found")
			return None


	# initializes adapter for FB fanpage
	def _initializeFbFanpage(self):
		return adapters.fb.fanpage.Fanpage(self.conf, self.db, self.conf.fb['limit'])

	def _initializeJobs(self):
		return adapters.jobs.Jobs(self.db)


	# runs code for the adapter based on command-line argument
	def RunAdapter(self, adapt):
		modes = {
			'--producent' : adapt.ProducePosts,
			'--addJob' : adapt.AddJob,
			'--crawler' : adapt.ProducePost
			}

		if sys.argv[1] in modes.keys():
			modes[sys.argv[1]]()


	# closes all DB connection and everything after we're done with the adapter
	def CloseAdapter(self, adapt):
		modes = {}

		if sys.argv[1] in modes.keys():
			modes[sys.argv[1]]()