from database import Database

def connect(path, verbose=False):
    if verbose:
        print "loading database from %s" % path
    d = Database(path)
    
    status = d.sanity_check()
    if verbose:
        print 'Sanity Check: %s' % str(status)

    count = d.get_current()
    if verbose:
        print "Replayed %i transactions." % count

    return d
