from util import __not_implemented__

class Row(object):
    def __init__(self, key, data):
        self.key = key
        self.location_type = 'file' # 'file' or 'packed_file'
        self.data = data
        self.changes = []
    def save(self, db):
        pass

    apply_statement = save = load = delete = __not_implemented__

class Statement(object):
    # single statement that will become part of a transaction.
    as_row = __not_implemented__

class Transaction(object):
    def __init__(self, parent):
        self.parent_rev = parent
        self.statements = []

    # Can be applied to a DB, and serialized/deserialized so that it can be replayed.
    apply = __not_implemented__


