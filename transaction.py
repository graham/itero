import json
import hashlib

class Transaction(object):
    def __init__(self, rev=None):
        #uuid = str(uuid.uuid4()).split('-')[-1]
        self.revision = str(time.time()) + str(rev)
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
