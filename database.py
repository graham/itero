import sys
import os
import time
import platform
import json
import hashlib
import shutil
import uuid

from transaction import Transaction
from row import Row
from statement import Statement

from util import __not_implemented__

VERBOSE = 0

class Database(object):
    "Base class for database, should never be instantiated on it's own."
    def __init__(self, data_path):
        self.revision = 0
        self.packed_file_size = 1024 * 1024 * 4

        #This will be updated by something else, Dropbox, rsync, etc.
        self.data_path = os.path.abspath(data_path)

        #local storage of key to data !! might not contain all keys !!
        self.objects = {}
        
        #local commit log
        self.init_log()
        self.transactions = []

    def init_log(self):
        self.commit_log_file = open(self.data_path + "/%s.%s.commit_log.txt" % (platform.node(), str(uuid.uuid4())), 'a')
        self.commit_log( 'init', self.revision, time.time(), time.asctime() )
        
    def sanity_check(self):
        add_dirs = ('', '/compact', '/data', '/transaction', '/checkpoints')
        for i in add_dirs:
            if not os.path.isdir(self.data_path + i):
                os.mkdir(self.data_path + i)
        return "we good"
                
    def create_clean_slate(self):
        add_dirs = ('', '/compact', '/data', '/transaction', '/checkpoints')
        for i in add_dirs:
            if os.path.isdir(self.data_path + i):
                shutil.rmtree(self.data_path + i)
        for i in add_dirs:
            os.mkdir(self.data_path + i)

        self.init_log()

    def commit_log(self, *args):
        self.commit_log_file.write(json.dumps(args) + '\n')
        self.commit_log_file.flush()

    def commit_transaction(self, transaction, is_replay=False):
        if not is_replay:
            transaction.save(self.data_path)

        h = transaction.hashed_data()

        if transaction.hashed_data() in self.transactions:
            return

        if not is_replay:
            self.commit_log("begin_transaction", transaction.revision, h)

        key_cache = {}

        for i in transaction.statements:
            rev_key = "%s_%s" % (i.target_key, i.target_hash)
            if rev_key in key_cache:
                prev = key_cache[rev_key]
            else:
                prev = self.get_revision(i.target_key, i.target_hash)

            if prev == None:
                if rev_key in key_cache:
                    prev = key_cache[rev_key]
                else:
                    prev = Row(i.target_key)

            new = prev.apply_statement(i)
            new.result_of = transaction.path()

            new.save_to(self.data_path)
            key_cache[rev_key] = new

            if not is_replay:
                self.commit_log( [i.target_key, prev.hashed_data(), new.hashed_data()] )

        if not is_replay:
            self.commit_log("end_transaction", transaction.revision, h)
        
        for i in key_cache:
            obj = key_cache[i]
            if obj.data:
                self.save_doc(obj)
            else:
                hk = self.gen_key(i.split('_')[0])
                rev = i.split('_')[1]
                drops = []
                for i in self.objects[hk]:
                    if i.hashed_data() == rev:
                        drops.append(i)
                for i in drops:
                    self.objects[hk].remove(i)
                if len(self.objects[hk]) == 0:
                    self.objects.pop( hk )
        
        self.transactions.append(transaction.hashed_data())
        return key_cache

    def find_new_transactions(self):
        p = self.data_path + "/transaction/"
        new = []
        for i in os.listdir(p):
            ts, rev, hash = i.split('_')
            if hash not in self.transactions:
                new.append(i)
        new.sort()
        return new

    def get_current(self):
        trans = self.find_new_transactions()
        for i in trans:
            t = Transaction.load(self.data_path, i)
            try:
                self.commit_transaction(t, is_replay=True)
            except:
                import traceback
                traceback.print_exc()
                print 'Failed Commit: %s' % t
        return len(trans)

    walk = get_current
            
    def find_common_ancestor(self, docs):
        ancestors = []
        
        for i in docs:
            ancestors += self.get_parents(i)

        revs = []

        for i in ancestors:
            revs.append(i.hashed_data())
            
        for i in revs:
            if revs.count(i) > 1:
                return i

        return self.find_common_ancestor(ancestors)

    def collect_statements_for(self, key, previous_rev, future_rev):
        statements = []
        cur = [self.get_revision(key, future_rev)]
        
        running = 1
        while running:
            for doc in cur:
                if doc.hashed_data() == previous_rev:
                    running = 0
                    break;
                else:
                    t = Transaction.load(self.data_path, doc.result_of)
                    for s in t.statements:
                        if key == s.target_key:
                            statements.append(s)
            new_cur = []
            for doc in cur:
                for par in self.get_parents(doc):
                    new_cur.append(par)
            cur = new_cur

        statements.reverse()
        return statements

    def attempt_merge(self, doc1, doc2):
        ancestor_hash = self.find_common_ancestor([doc1, doc2])
        ancestor = self.get_revision(doc1.key, ancestor_hash)

        statements1 = self.collect_statements_for( doc1.key, ancestor_hash, doc1.hashed_data() )
        statements2 = self.collect_statements_for( doc2.key, ancestor_hash, doc2.hashed_data() )
        
        first_try = ancestor.apply_statements(statements1).apply_statements(statements2)
        second_try = ancestor.apply_statements(statements2).apply_statements(statements1)

        if first_try.hashed_data() == second_try.hashed_data():
            t = self.transaction()
            t.add( doc2.statement('drop', []) )

            for i in statements2:
                t.add( doc1.statement( i.command, i.args ) )

            self.commit_transaction(t)
            return True
        else:
            return False

    def execute(self, key, action, args):
        rows = self.get_rows(key)
        if len(rows) == 0:
            row = Row(key)
            t = self.transaction()
            t.add( row.statement(action, args) )
            self.commit_transaction(t)
        elif len(rows) == 1:
            row = rows[0]
            t = self.transaction()
            t.add( row.statement(action, args) )
            self.commit_transaction(t)
        else:
            raise Exception("too many docs!!!")                   

    def transaction(self):
        t = Transaction(self.revision)
        self.revision += 1
        return t

    def compact(self):
        pass

    def gen_key(self, key):
        hk = hashlib.sha256()
        hk.update(key)
        return hk.hexdigest()

    def get_rows(self, key):
        hk = self.gen_key(key)
        if hk in self.objects:
            return self.objects[hk]
        else:
            return []

    def get_revision(self, key, revision):
        hk = self.gen_key(key)
        if hk in self.objects:
            for i in self.objects[hk]:
                if i.hashed_data() == revision:
                    return i
            if Row.exists(self.data_path, key, revision):
                row = Row.load_from(self.data_path, key, revision)
                return row
            return None
        else:
            return None

    def get_parents(self, row):
        rows = []
        for i in row.parents:
            rows.append( self.get_revision(row.key, i) )
        return rows

    def save_doc(self, doc):
        hk = self.gen_key(doc.key)
        if hk in self.objects:
            keep = []
            revs = []
            for i in self.objects[hk]:
                if i.hashed_data() in doc.parents:
                    pass
                else:
                    if i.hashed_data() not in revs:
                        keep.append(i)
                        revs.append(i.hashed_data())

            if doc.hashed_data() not in revs:
                keep.append(doc)
            else:
                for i in revs:
                    if i.hashed_data() == doc.hashed_data():
                        i.parents += doc.parents
            
            self.objects[hk] = keep
            return keep
        else:
            self.objects[hk] = [doc]
            return self.objects[hk]

    def drop_doc(self, doc):
        hk = self.gen_key(doc.key)
        if hk in self.objects:
            drop = []
            for i in self.objects[hk]:
                if i.hashed_data() == doc.hashed_data():
                    drop.append(i)
            for i in drop:
                self.objects[hk].remove(i)
        else:
            pass

    def filter(self, func):
        hits = []
        for i in self.objects:
            for doc in self.objects[i]:
                try:
                    hit = func(doc)
                except:
                    hit = False

                if hit:
                    hits.append(doc)
        return hits
            
    def keys(self):
        keys = []
        for i in self.objects:
            first = self.objects[i][0]
            keys.append(first.key)
        return keys
        
    def get(self, key):
        return [i.data for i in self.get_rows(key)]
    def set(self, key, value):
        self.execute(key, 'set', value)
    def update(self, key, value):
        self.execute(key, 'update', value)
    def drop(self, key):
        self.execute(key, 'drop', [])

    def __iter__(self):
        keys = self.objects.keys()
        def gen(keys):
            for i in keys:
                if i in self.objects:
                    yield self.objects[i]
        return gen(keys)

