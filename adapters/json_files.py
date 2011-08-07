from database import Database

import time
import json
import os

class JSONDatabase(Database):
    initial_state_data = {
        'last_compact': int(time.time()),
        'current_revision': 0,
        }
    def initialize(self):
        if not os.path.isdir(self.data_path):
            os.mkdir(self.data_path)
        else:
            pass

        if not os.path.isfile(self.local_state_filename):
            f = open(self.local_state_filename, 'w')
            f.write( json.dumps( JSONDatabase.initial_state_data ) )
            f.close()
        else:
            self.local_config = json.loads( open(self.local_state_filename).read() )

    def commit(self, commit_objects):
        pass


from commit import Statement, Transaction

class JSONStatement(Statement):
    def __init__(self, key, action, values):
        self.key = key
        self.action = action
        self.values = values

class JSONTransaction(Transaction):
    pass
