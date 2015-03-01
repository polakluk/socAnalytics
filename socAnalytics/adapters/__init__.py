import adapters.fb.fanpage

# helper class for proper initialization of an adapter
class Helper:

	# contructor
	def __init__(self, conf, currentDb):
		self.db = currentDb
		self.conf = conf


	# returns adapter based on config
	def GetAdapter(self):
		adapters = {}
		adapters["fbFanpage"] = self._initializeFbFanpage

		if self.conf.adapter["type"] in adapters.keys():
			return adapters[self.conf.adapter["type"]]()
		else:
			return None


	# initializes adapter for FB fanpage
	def _initializeFbFanpage(self):
		return adapters.fb.fanpage.Fanpage(self.conf, self.db, self.conf.fb['limit'])