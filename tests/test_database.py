import unittest
import os
import sys
import shutil
import platform
import random
sys.path.append('.')


from database import Database
from transaction import Transaction
from statement import Statement
from row import Row

import random
import string
import time

prev_keys = ['']
def gen():
    key = ''
    while key in prev_keys:
        key = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(8))
    prev_keys.append(key)
    return key

TEST_COUNT = 4

class TestJSONDatabase(unittest.TestCase):
    data_path = './sandbox/'
    add_dirs = ('', '/compact', '/data', '/transaction', '/conflict')
    is_lazy = False

    def connect(self):
        x = Database(TestJSONDatabase.data_path)
        return x

    def init(self):
        for i in TestJSONDatabase.add_dirs:
            if os.path.isdir(TestJSONDatabase.data_path + i):
                shutil.rmtree(TestJSONDatabase.data_path + i)
        for i in TestJSONDatabase.add_dirs:
            os.mkdir(TestJSONDatabase.data_path + i)

    def test_create(self):
        self.init()
        x = self.connect()
        self.assertTrue( os.path.isfile(TestJSONDatabase.data_path + "%s.commit_log.txt" % platform.node()) )
    
    def test_put(self):
        self.init()
        x = self.connect()
        t = x.transaction()
        r = Row('test')
        t.add( r.statement('update', {'one':1}) )
        x.commit_transaction(t)
    
        rows = x.get_rows('test')
        
        self.assertEqual([i.data for i in rows], [{'one':1}])
    
    def test_put_two(self):
        self.init()
        x = self.connect()
        t = x.transaction()
        r = Row('test')
        t.add( r.statement('update', {'one':1}) )
        x.commit_transaction(t)
    
        rows = x.get_rows('test')
    
        self.assertEqual([i.data for i in rows], [{'one':1}])
        self.assertEqual(len(rows), 1)
    
        row = rows[0]
    
        t = x.transaction()
        t.add( row.statement('update', {'two':2}) )
        x.commit_transaction(t)
    
        rows = x.get_rows('test')
        
        self.assertEqual([i.data for i in rows], [{'one':1, 'two':2}])
    
    def test_easy_put(self):
        self.init()
        x = self.connect()
        
        x.execute('test', 'update', {'one':1})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1}] )
    
        x.execute('test', 'update', {'two':2})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1, 'two':2}] )
    
    def test_search(self):
        self.init()
        x = self.connect()
        
        x.execute('test', 'update', {'one':1})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1}] )
    
        x.execute('test', 'update', {'two':2})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1, 'two':2}] )
    
        s = x.filter( lambda doc: True if 'two' in doc.data else False )
        self.assertEqual( [i.data for i in s], [{'one':1, 'two':2}] )
    
        s = x.filter( lambda doc: True if 'test' == doc.key else False )
        self.assertEqual( [i.key for i in s], ['test'] )
    
    def test_replay_and_search(self):
        self.init()
        x = self.connect()
        
        x.execute('test', 'update', {'one':1})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1}] )
    
        x.execute('test', 'update', {'two':2})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1, 'two':2}] )
    
        s = x.filter( lambda doc: True if 'two' in doc.data else False )
        self.assertEqual( [i.data for i in s], [{'one':1, 'two':2}] )
    
        s = x.filter( lambda doc: True if 'test' == doc.key else False )
        self.assertEqual( [i.key for i in s], ['test'] )
    
        y = self.connect()
        y.get_current()
        s = y.filter( lambda doc: True if 'test' == doc.key else False )
        self.assertEqual( [i.key for i in s], ['test'] )
    
        self.assertEqual( [i.data for i in y.get_rows('test')], [{'one':1, 'two':2}] )
        
    def test_many_keys(self):
        bunch_o_keys = [(i, gen()) for i in range(20)]
        
        self.init()
        x = self.connect()
    
        value = {}
        
        for index, i in bunch_o_keys:
            x.execute('test', 'update', {i:index})
            value.update({i:index})
    
        data = x.get_rows('test')
        data = data[0].data
        
        self.assertEqual(data, value)
    
    def test_many_keys_single_transaction(self):
        bunch_o_keys = [(i, gen()) for i in range(20)]
        
        self.init()
        x = self.connect()
    
        value = {}
        
        t = x.transaction()
        
        for index, i in bunch_o_keys:
            r = Row('test')
            t.add( r.statement('update', {i:index}) )
            value.update({i:index})
    
        x.commit_transaction(t)
    
        data = x.get_rows('test')
        data = data[0].data
        
        self.assertEqual(data, value)
        
    def test_many_keys_many_transaction_with_replay(self):
        bunch_o_keys = [(i, gen()) for i in range(50)]
        self.init()
        x = self.connect()

        value = {}
        for i in range(10):
            t = x.transaction()

            for index, i in bunch_o_keys:
                r = Row('test')
                t.add( r.statement('update', {i:index}) )
                value.update({i:index})

            x.commit_transaction(t)

        data = x.get_rows('test')
        data = data[0].data
        self.assertEqual(data, value)
        
        y = self.connect()
        y.get_current()
        ydata = y.get_rows('test')
        ydata = ydata[0].data
        self.assertEqual(ydata, value)
    
    def test_key_drop(self):
        self.init()
        x = self.connect()
        
        x.execute('test', 'update', {'one':1})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1}] )
    
        x.execute('test', 'drop', [])
        self.assertEqual( x.get_rows('test'), [] )
    
    def test_bad_search(self):
        self.init()
        x = self.connect()
        
        x.execute('test', 'update', {'one':1})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1}] )
    
        s = x.filter( lambda doc: True if 'test' == doc.key else False )
        self.assertEqual( [i.key for i in s], ['test'] )
    
        s = x.filter( lambda doc: True if doc.data['one'] == 1 else False )
        self.assertEqual( [i.key for i in s], ['test'] )
    
        s = x.filter( lambda doc: True if doc.data['two'] == 2 else False )
        self.assertEqual( [i.key for i in s], [] )
    
    def test_get_current(self):
        self.init()
        x = self.connect()
        
        x.execute('test', 'update', {'one':1})
        x.execute('test', 'update', {'two':2})
        self.assertEqual( [i.data for i in x.get_rows('test')], [{'one':1, 'two':2}] )
    
        y = self.connect()
        y.get_current()
        y.execute('test', 'update', {'three':3})
        x.execute('test', 'update', {'four':4})
    
        x.get_current()
        y.get_current()
        
        x_rows = [i.data for i in x.get_rows('test')]
        y_rows = [i.data for i in y.get_rows('test')]
        
        self.assertTrue( {'four': 4, 'one': 1, 'two': 2} in x_rows )
        self.assertTrue( {'four': 4, 'one': 1, 'two': 2} in y_rows )
        
        self.assertTrue( {'one': 1, 'three': 3, 'two': 2} in x_rows )
        self.assertTrue( {'one': 1, 'three': 3, 'two': 2} in y_rows )

    def test_find_ancestor(self):
        self.init()
        
        x = self.connect()
        y = self.connect()

        x.update('test', {'one':1})
        y.get_current()

        self.assertEqual( x.get('test'), y.get('test') )

        parent = x.get_rows('test')[0]

        x.update('test', {'two':2})
        x.update('test', {'even':True})

        y.update('test', {'three':3})
        y.update('test', {'odd':True})

        x.get_current()
        y.get_current()

        rows = x.get_rows('test')
        parent_hash = x.find_common_ancestor(rows)
        self.assertEqual(parent_hash, parent.hashed_data())

    def test_auto_merge(self):
        self.init()
        
        x = self.connect()
        y = self.connect()

        x.update('test', {'one':1})
        y.get_current()

        self.assertEqual( x.get('test'), y.get('test') )

        parent = x.get_rows('test')[0]

        x.update('test', {'two':2})
        x.update('test', {'even':True})

        y.update('test', {'three':3})
        y.update('test', {'odd':True})

        x.get_current()
        y.get_current()

        rows = x.get_rows('test')
        parent_hash = x.find_common_ancestor(rows)
        self.assertEqual(parent_hash, parent.hashed_data())

        left, right = x.get_rows('test')
        
        self.assertTrue(x.attempt_merge(left, right))
        self.assertEqual( [{'even': True, 'one': 1, 'odd': True, 'three': 3, 'two': 2}], x.get('test') )

        x.get_current()
        y.get_current()

        [left] = x.get('test')
        self.assertTrue( left in y.get('test') )
        
        [left] = y.get('test')
        self.assertTrue( left in x.get('test') )

    def test_auto_merge_failed(self):
        self.init()
        
        x = self.connect()
        y = self.connect()

        x.update('test', {'one':1})
        y.get_current()

        self.assertEqual( x.get('test'), y.get('test') )

        parent = x.get_rows('test')[0]

        x.update('test', {'two':2})
        x.update('test', {'even':True})

        y.update('test', {'three':3})
        y.update('test', {'even':False})

        x.get_current()
        y.get_current()

        rows = x.get_rows('test')
        parent_hash = x.find_common_ancestor(rows)
        self.assertEqual(parent_hash, parent.hashed_data())

        left, right = x.get_rows('test')
        
        self.assertFalse(x.attempt_merge(left, right))
        self.assertTrue( {'even': True, 'two': 2, 'one': 1} in x.get('test') )
        self.assertTrue( {'even': False, 'three': 3, u'one': 1} in x.get('test') )

        x.get_current()
        y.get_current()

        [left, right] = x.get('test')
        self.assertTrue( left in y.get('test') )
        self.assertTrue( right in y.get('test') )
        
        [left, right] = y.get('test')
        self.assertTrue( left in x.get('test') )
        self.assertTrue( right in x.get('test') )

    def test_collect_statements(self):
        self.init()
        
        x = self.connect()
        y = self.connect()

        x.update('test', {'one':1})
        y.get_current()

        self.assertEqual( x.get('test'), y.get('test') )

        parent = x.get_rows('test')[0]

        x.update('test', {'two':2})
        x.update('test', {'even':True})

        y.update('test', {'three':3})
        y.update('test', {'odd':True})

        x.get_current()
        y.get_current()

        rows = x.get_rows('test')
        parent_hash = x.find_common_ancestor(rows)
        self.assertEqual(parent_hash, parent.hashed_data())

        conflict_rows = x.get('test')
        
        self.assertTrue( {'even': True, 'two': 2, 'one': 1} in conflict_rows )
        self.assertTrue( {'odd': True, 'three': 3, u'one': 1} in conflict_rows )

if __name__ == '__main__':
    unittest.main()
