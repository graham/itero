import sys
import os
import time
import platform
import json
import hashlib
import shutil

from commit import Transaction, Statement, Row
from util import __not_implemented__

VERBOSE = 0

class Database(object):
    "Base class for database, should never be instantiated on it's own."
    def __init__(self, data_path):
        self.revision = 0

        #This will be updated by something else, Dropbox, rsync, etc.
        self.data_path = os.path.abspath(data_path)

        #local storage of key to data !! might not contain all keys !!
        self.objects = {}

        #local commit log
        self.commit_log_file = open(self.data_path + "/%s.commit_log.txt" % platform.node(), 'a')
        self.commit_log( 'init', self.revision, time.time(), time.asctime() )
        self.transactions = []

    def create_clean_slate(self):
        add_dirs = ('', '/compact', '/data', '/transaction', '/conflict')
        for i in add_dirs:
            if os.path.isdir(self.data_path + i):
                shutil.rmtree(self.data_path + i)
        for i in add_dirs:
            os.mkdir(self.data_path + i)

        self.commit_log_file = open(self.data_path + "/%s.commit_log.txt" % platform.node(), 'a')
        self.commit_log( 'init', self.revision, time.time(), time.asctime() )

    def load_from_log(self):
        replay_commit_log_file = open(self.data_path + "/%s.commit_log.txt" % platform.node(), 'r')

        while replay_commit_log_file:
            i = replay_commit_log_file.readline()
            if i == '':
                replay_commit_log_file = None
                continue

            j = json.loads(i)
            if j[0] in ('init', 'create_conflict'):
                pass # these are basically comments for now.
            elif j[0] == 'begin_transaction':
                rev, id = j[1:]
                t = Transaction.load(self.data_path, rev, id)
                self.commit_transaction(t, is_replay=True)

                adds = {}
                next_line = replay_commit_log_file.readline()
                while next_line:
                    cadd = json.loads( next_line )
                    if cadd[0] == 'end_transaction':
                        assert cadd[2] == id
                        next_line = None
                    else:
                        adds[cadd[1]] = cadd[2]
                        next_line = replay_commit_log_file.readline()

                for key in adds:
                    row = self.get_row(key)
                    assert adds[key] == row.hash_data()

    def commit_log(self, *args):
        self.commit_log_file.write(json.dumps(args) + '\n')
        self.commit_log_file.flush()
        
    def handle_conflict(self, statement):
        pass

    def commit_transaction(self, transaction, is_replay=False):
        h = transaction.hash_data()
        if not is_replay:
            self.commit_log("begin_transaction", transaction.revision, h)

        d = {}
        for i in transaction.statements:
            cur = self.get_row(i.target_key)
            if cur == None:
                if self.gen_key(i.target_key) in d:
                    cur = d[self.gen_key(i.target_key)]
                else:
                    cur = Row(i.target_key, {})

            n = cur.apply_statement(i)

            #if i.data_hash:
            #    if (cur.hash_data() != i.data_hash):
            #        self.commit_log("create_conflict", transaction.revision)
            #        transaction.save_conflict(self.data_path)
            #        n = Row.load_hash_as(self.data_path, i.data_hash, i.target_key)

            if not is_replay:
                self.commit_log('update', i.target_key, n.hash_data())

            d[n.hash_key()] = n
            
        if not is_replay:
            self.commit_log("end_transaction", transaction.revision, h)

        for i in d:
            d[i].save(self.data_path)

        self.objects.update(d)
        self.transactions.append(transaction.hash_data())

    def find_new_transactions(self):
        p = self.data_path + "/transaction/"
        new = []
        for i in os.listdir(p):
            rev, hash = i.split('_')
            if hash not in self.transactions:
                new.append(i)
        new.sort()
        return new

    def get_current(self):
        trans = self.find_new_transactions()
        for i in trans:
            id, hash = i.split('_')
            t = Transaction.load(self.data_path, id, hash)
            self.commit_transaction(t, is_replay=True)

    def execute(self, *args, **kwargs):
        t = self.transaction()
        t.add( self.statement( *args, **kwargs ) )
        t.save(self.data_path)
        self.commit_transaction(t)

    def transaction(self):
        t = Transaction(self.revision)
        self.revision += 1
        return t

    def statement(self, for_key, *args, **kwargs):
        k = self.get_row(for_key)
        if not k:
            return Statement(for_key, '', *args, **kwargs)
        else:
            return Statement(for_key, k.hash_data(), *args, **kwargs)

    def get(self, key):
        row = self.get_row(key)
        if row:
            return row.data
        else:
            return None

    def get_row(self, key):
        hk = self.gen_key(key)
        if hk in self.objects:
            return self.objects[hk]
        else:
            return None

    def gen_key(self, key):
        hk = hashlib.sha256()
        hk.update(key)
        return hk.hexdigest()
