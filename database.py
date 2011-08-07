import sys
import os
import time

VERBOSE = 0

class Database(object):
    "Base class for database, should never be instantiated on it's own."
    def __init__(self, local_state_filename, data_path):
        #This should never be modified by anyone else, just this machine (likely one thread).
        self.local_state_filename = os.path.abspath(local_state_filename)
        self.local_config = {}

        #This will be updated by something else, Dropbox, rsync, etc.
        self.data_path = os.path.abspath(data_path)

        #Lets get this show on the road!
        self.initialize()

    def __not_implemented__(self): 
        raise Exception("Someone didn't write this function")
    initialize = checkpoint = compact = commit = __not_implemented__


    
        
        
class SQLiteDatabase(Database):
    pass

class KeyValueDatabase(Database):
    pass


