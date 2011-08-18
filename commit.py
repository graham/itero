from util import __not_implemented__

import hashlib
import json
import time
import uuid

class Row(object):
    def __init__(self, key, data):
        self.key = str(key)
        self.type = 'dict'
        self.data = data

    def __repr__(self):
        return "<%s key:%s type:%s data:%r>" % (self.__class__.__name__, self.key, self.type, self.data)
        
    def hash_key(self):
        return hashlib.sha256(self.key).hexdigest()

    def get_data(self):
        keys = self.data.keys()
        keys.sort()
        data = [self.data[i] for i in keys]
        return [self.type, [keys, data]]

    def hash_data(self):
        return hashlib.sha256( json.dumps(self.get_data()) ).hexdigest()

    def apply_statement(self, statement):
        row = self
        data = row.data.copy()

        if statement.command == 'update':
            s = {}
            for i in statement.args:
                s[str(i)] = statement.args[i]
            data.update( s )
        elif statement.command == 'pop':
            for i in statement.args:
                data.pop(i)
        elif statement.command == 'drop':
            data = None

        r = Row(row.key, data)
        return r

    def save(self, root):
        f = open(root + '/data/' + self.hash_data(), 'w')
        f.write( json.dumps(self.get_data()) )
        f.close()

    @classmethod
    def load_hash_as(cls, root, hash, key):
        row = Row(key, {})
        f = open(root + '/data/' + hash, 'r')
        type, all_data = json.loads(f.read())
        keys, data = all_data
        for kkey, value in zip(keys, data):
            row.data[kkey] = value
        return row
        
    @classmethod
    def load(cls, root, key):
        row = Row(key, {})

        hkey = hashlib.sha256(key).hexdigest()
        f = open(root + '/data/' + hkey, 'r')
        type, all_data = json.loads(f.read())
        keys, data = all_data
        for key, value in zip(keys, data):
            row.data[key] = value

        return row

class Statement(object):
    def __init__(self, target_key, data_hash, command, args):
        self.target_key = target_key
        self.data_hash = data_hash
        self.command = command
        self.args = args

    def __repr__(self):
        return self.as_json()

    def as_json(self):
        return json.dumps( [self.target_key, self.data_hash, self.command, self.args] )

    @classmethod
    def from_json(self, s):
        return Statement(*json.loads(s))
        
class Transaction(object):
    def __init__(self, rev=None):
        #uuid = str(uuid.uuid4()).split('-')[-1]
        self.revision = str(int(time.time())) + str(rev)
        self.statements = []

    def __repr__(self):
        return "<Transaction\n%s>" % [str(i) for i in self.statements]
        
    def add(self, statement):
        self.statements.append(statement)

    def hash_data(self):
        h = hashlib.sha256()
        for i in self.statements:
            h.update( i.target_key )
            h.update( i.data_hash )
            h.update( i.command )
            h.update( str(i.args) )
        return h.hexdigest()

    def save_conflict(self, root):
        f = open(root + '/conflict/%s_%s' % (self.revision, self.hash_data()), 'w')
        for i in self.statements:
            f.write( i.as_json() + '\n' )
        f.close()

    def save(self, root):
        f = open(root + '/transaction/%s_%s' % (self.revision, self.hash_data()), 'w')
        for i in self.statements:
            f.write( i.as_json() + '\n' )
        f.close()
        
    @classmethod
    def load(cls, root, rev, hash):
        t = Transaction(rev)
        f = open(root + '/transaction/%s_%s' % (rev, hash))
        d = f.read()
        for i in d.split('\n'):
            i = i.strip()
            if i:
                t.statements.append( Statement.from_json(i) )
        return t
