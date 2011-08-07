from util import __not_implemented__

class Statement(object):
    # single statement that will become part of a transaction.
    pass

class Transaction(object):
    def __init__(self, parent):
        self.parent_rev = parent
        self.statements = []

    # Can be applied to a DB, and serialized/deserialized so that it can be replayed.
    apply = __not_implemented__


