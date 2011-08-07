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
        for p in ('', '/compact', '/file', '/transaction'):
            if not os.path.isdir(self.data_path + p):
                os.mkdir(self.data_path + p )
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

    def execute(self, key, action, values):
        t = self.transaction()
        t.add( self.statement( key, action, values ) )
        return t.apply(self)
        
    def get(self, key):
        if key in self.objects:
            x = self.objects[key].data
            if x:
                return x.copy()
            else:
                return None
        else:
            return None

from commit import Statement, Transaction, Row

class JSONRow(Row):
    def apply_statement(self, statement):
        data = self.data.copy()
        self.changes.append( statement.as_row() )
        if statement.action == 'update':
            data.update( statement.values )
        elif statement.action == 'pop':
            for i in statement.values:
                data.pop(i)
        elif statement.action == 'drop':
            data = None

        return data

    def save(self, db):
        f = open(db.data_path + '/file/' + self.key, 'w')
        f.write( json.dumps(self.data) )
        f.close() 
        self.changes = []

    def load(self, db, key):
        f = open(db.data_path + '/file/' + key, 'w')
        self.data = json.loads( f.read() )
        if self.data == None:
            self.data = {}
        self.key = key
        
class JSONStatement(Statement):
    def __init__(self, key, action, values):
        self.key = key
        self.action = action
        self.values = values

    def as_row(self):
        return json.dumps( [self.key, self.action, self.values] )

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
            o = JSONRow(i.key, new_d)
            o.save(db)
            db.objects[i.key] = o
            

        
