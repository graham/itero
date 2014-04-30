import json
import hashlib
import time
from statement import Statement

import os

class Transaction(object):
    def __init__(self, rev=None):
        self.ts = str("%.8f" % time.time())
        self.revision = str(rev)
        self.statements = []

    def __repr__(self):
        return "<Transaction\n%s>" % [str(i) for i in self.statements]
        
    def add(self, statement):
        self.statements.append(statement)

    def hashed_data(self):
        h = hashlib.sha256()
        for i in self.statements:
            h.update( i.target_key + i.target_hash + i.command )
        return h.hexdigest()

    def path(self):
        return '%s_%s_%s' % (self.ts, self.revision, self.hashed_data())

    def save(self, root):
        f = open(root + '/transaction/' + self.path(), 'w')
        for i in self.statements:
            f.write( i.as_json() + '\n' )
        f.close()
        
    @classmethod
    def load(cls, root, path):
        ts, rev, hash = path.split("_")
        t = Transaction(rev)
        t.ts = ts
        f = open(root + '/transaction/%s' % path)
        d = f.read()
        for i in d.split('\n'):
            i = i.strip()
            if i:
                t.statements.append( Statement.from_json(i) )
        return t

    @classmethod
    def exists(cls, root, path):
        return os.path.exists(root + '/transaction/' + path)
