import sqlite3

# class that manages DB connection
class Db:

	# opens a connection to a file with SQLite3 DB
	def __init__(self, f):
		self.db_file = f
		self.conn = sqlite3.connect(self.db_file)


	# commits data to the DB file
	def commit(self):
		self.conn.commit()


	# closes connection to the DB
	def close(self):
		self.conn.close()