************************************
*            The Basics            *
************************************

I wrote this over a couple weekends (ok, i re-wrote it each weekend).

It's pretty un-optimized, and really I tried to make things easy to understand and functional rather than fast. For now, at least with something like Dropbox it works quite well.

The semantics are pretty simple, for now:
    
    1. It's a key value store, keys are strings, values are dictionaries (sorry, more data types to come).
    2. For now you only have basic operations (update, drop, pop, set)
       Drop/Set work on keys, update/pop work on keys within values.
    3. State is maintained by the db object, so whenever you "connect" to the database it replays all the transactions to get back to the current state.

Example:
        computer1: db = itero.connect('~/Dropbox/test/')
        computer2: db = itero.connect('~/Dropbox/test/')
        
        computer1: db.update('test', {'name':'Graham', 'language':'python'})
        computer1: print db.get('test')
                   [{'name':'Graham', 'language':'python'}]

        computer2: db.get_current()
        computer2: print db.get('test')
                   [{'name':'Graham', 'language':'python'}]

More explaination:
    
