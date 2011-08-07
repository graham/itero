class Statement(object):
    # single statement that will become part of a transaction.
    pass

class Transaction(object):
    # Can be applied to a DB, and serialized/deserialized so that it can be replayed.
    pass


