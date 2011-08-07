import sys
import os

VERBOSE = 0
if '-v' in sys.argv:
    VERBOSE = 1

class Database(object):
    "Base class for database, should never be instantiated on it's own."
    def __init__(self, local_state, data_path):
        self.local_state = local_state
        self.data_path = os.path.abspath(data_path)
        self.initialize()

    def __not_implemented__(self): 
        raise Exception("Someone didn't write this function")
    initialize = checkpoint = compact = commit = __not_implemented__

import json

class JSONDatabase(Database):
    def initialize(self):
        if not os.path.exists(self.data_path):
            if VERBOSE: 
                print 'Creating %s' % self.data_path
            os.mkdir(self.data_path)
        else:
            if VERBOSE:
                print 'Loading DB Config...'
        
class SQLiteDatabase(Database):
    pass

class KeyValueDatabase(Database):
    pass


