import adapters
import adapters.fb.fanpage
import config
import db

conf = config.Config()
currentDb = db.Db(conf.db['file'])
adapters = adapters.Helper(conf, currentDb)

adapter = adapters.GetAdapter()
adapters.RunAdapter(adapter)
adapters.CloseAdapter(adapter)

currentDb.Close()