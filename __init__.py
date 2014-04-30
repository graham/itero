from database import Database
from kvql import dig, digs

def connect(path, verbose=False):
    if verbose:
        print "loading database from %s" % path
    d = Database(path)
    
    status = d.sanity_check()
    if verbose:
        print 'Sanity Check: %s' % str(status)

    d.replay_compacted()
    print 'compacted replay done'

    count = d.get_current()
    if verbose:
        print "Replayed %i transactions." % count

    return d
