class Config:

	# constructor
	def __init__(self):
		self.debug = True

		# FB creds
		self.fb = {}
		self.fb['app_id'] = 302660193219713
		self.fb['app_secret'] = "0a953030f5f60ba2ac975173787943bc"
		self.fb['limit'] = 100

		# DB creds
		self.db = {}
		self.db['file'] = "db.sqlite"

		# adapters
		self.adapter = {}
		self.adapter["type"] = "fbFanpage"