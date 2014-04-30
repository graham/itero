from util import __not_implemented__

import hashlib
import json
import time
import uuid
import os
import platform

from statement import Statement

SUPPORTED_DATA_TYPES = [ dict, ]

class Row(object):
    def __init__(self, key, parents=None, data=None, result_of=None):
        self.result_of = result_of
        self.origin = platform.node().split('.')[0]

        if data == None:
            self.data = {}
        else:
            self.data = data

        assert type(self.data) in SUPPORTED_DATA_TYPES

        if parents:
            self.parents = parents
        else:
            self.parents = []

        self.key = str(key)
        self.type = 'dict'

    def statement(self, *args, **kwargs):
        return Statement(self.key, self.hashed_data(), *args, **kwargs)

    def __repr__(self):
        return "<%s key:%s parents:%s type:%s data:%r>" % (self.__class__.__name__, self.key, self.parents, self.type, self.data)
        
    def hashed_key(self):
        return hashlib.sha256(self.key).hexdigest()

    def get_data(self):
        if self.type == 'dict':
            keys = self.data.keys()
            keys.sort()
            data = [self.data[i] for i in keys]
            d = [self.type, self.key, self.parents, [keys, data]]
            return d
            
    def hashed_data(self):
        keys = self.data.keys()
        keys.sort()
        data = [self.data[i] for i in keys]
        return hashlib.sha256( json.dumps([keys, data]) ).hexdigest()

    def apply_statements(self, statements):
        cur = self
        for i in statements:
            cur = cur.apply_statement(i)
        return cur

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
        elif statement.command == 'set':
            data = statement.args

        parents = []

        return Row(row.key, parents=[statement.target_hash], data=data)

    def checksum(self):
        return self.hashed_data()[-6:]

    def save_to(self, path):
        subdir = self.hashed_key()[:2]
        if not os.path.isdir(path + '/data/' + subdir):
            os.mkdir(path + '/data/' + subdir)
        f = open(path + '/data/' + subdir + '/' + self.hashed_key() + "_" + self.hashed_data(), 'w')
        f.write(json.dumps(self.get_data()))
        f.write("\n")
        f.write(self.result_of)
        f.close()

    @classmethod
    def load_from(self, path, key, rev):
        row = Row(key)
        subdir = row.hashed_key()[:2]
        f = open(path + '/data/' + subdir + '/' + row.hashed_key() + "_" + rev)
        prev_data, prev_transaction = f.read().split('\n')
        [row.type, row.key, row.parents, [keys, data]] = json.loads(prev_data)
        row.result_of = prev_transaction
        row.data = {}
        for k, d in zip(keys, data):
            row.data[k] = d
        return row
        
    @classmethod
    def exists(self, path, key, rev):
        row = Row(key)
        subdir = row.hashed_key()[:2]
        return os.path.exists(path + '/data/' + subdir + '/' + row.hashed_key() + "_" + rev)
        
