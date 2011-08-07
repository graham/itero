from database import Database

import time
import json
import os

class JSONDatabase(Database):
    initial_state_data = {
        'last_compact': int(time.time()),
        'revision': 0,
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
            
        #keys to data (although data may be in a file, or in a compacted file)
        self.objects = {}
        
    def transaction(self):
        return JSONTransaction(self.local_config['revision'])

    def statement(self, *args, **kwargs):
        return JSONStatement(*args, **kwargs)

    def keys(self):
        return self.objects.keys()

    #quick and dirty
    def execute(self, key, action, values):
        t = self.transaction()
        t.add( self.statement( key, action, values ) )
        return t.apply(self)
        
    def get(self, key):
        if key in self.objects:
            return self.objects[key].data
        else:
            return None

from commit import Statement, Transaction

class JSONRow(object):
    def __init__(self, key, data):
        self.key = key
        self.location_type = 'file' # 'file' or 'packed_file'
        self.data = data

    def apply_statement(self, statement):
        data = self.data.copy()
        if statement.action == 'update':
            data.update( statement.values )
        elif statement.action == 'pop':
            for i in statement.values:
                data.pop(i)
        elif statement.action == 'drop':
            return None

        return data

    def save(self):
        pass
    def load(self):
        pass
    def delete(self):
        pass

class JSONStatement(Statement):
    def __init__(self, key, action, values):
        self.key = key
        self.action = action
        self.values = values

    def as_row(self):
        return json.dumps( [key, action, values] )

class JSONTransaction(Transaction):
    def add(self, statement):
        self.statements.append(statement)

    def apply(self, db):
        for i in self.statements:
            d = JSONRow(i.key, {})
            if i.key in db.objects:
                d = db.objects[i.key]

            new_d = d.apply_statement(i)
            if new_d == None:
                db.objects.pop(i.key)
                
            db.objects[i.key] = JSONRow(i.key, new_d)

        
