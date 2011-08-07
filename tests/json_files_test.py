import unittest
import os
import sys
import shutil
sys.path.append('.')

from adapters.json_files import *

class TestJSONDatabase(unittest.TestCase):
    data_path = './sandbox/json_test/'
    state_path = './sandbox/json_state.db'
    add_dirs = ('', '/compact', '/file', '/transaction')

    def init(self):
        for i in TestJSONDatabase.add_dirs:
            if os.path.isdir(TestJSONDatabase.data_path + i):
                shutil.rmtree(TestJSONDatabase.data_path + i)
        if os.path.isfile(TestJSONDatabase.state_path):
            os.remove(TestJSONDatabase.state_path)

        self.assertFalse(os.path.isdir(TestJSONDatabase.data_path))
        self.assertFalse(os.path.isfile(TestJSONDatabase.state_path))

    def test_create_db(self):
        self.init()
        x = JSONDatabase(TestJSONDatabase.state_path, TestJSONDatabase.data_path)
        self.assertTrue(os.path.isdir(TestJSONDatabase.data_path))
        self.assertTrue(os.path.isfile(TestJSONDatabase.state_path))
        self.assertNotEqual(x, None)

    def test_load_db(self):
        x = JSONDatabase(TestJSONDatabase.state_path, TestJSONDatabase.data_path)
        self.assertTrue(os.path.isdir(TestJSONDatabase.data_path))
        self.assertTrue(os.path.isfile(TestJSONDatabase.state_path))
        self.assertNotEqual(x, None)

    def test_create_key(self):
        key = 'test_key'
        value = {'one':1, 'two':2}
        x = JSONDatabase(TestJSONDatabase.state_path, TestJSONDatabase.data_path)
        x.execute(key, "update", value)
        self.assertNotEqual( x.get(key), None )
        self.assertEqual( x.get(key), value )

    def test_key_update(self):
        key = 'test_key'

        value = {'one':1}
        value2 = {'one':1, 'two':2}
        value3 = {'one':951, 'two':2}
        value4 = {'one':951, 'two':2, 'three':'fuck you'}

        update2 = {'two':2}
        update3 = {'one':951}
        update4 = {'three':'fuck you'}

        x = JSONDatabase(TestJSONDatabase.state_path, TestJSONDatabase.data_path)
        x.execute(key, "update", value)
        self.assertNotEqual( x.get(key), None )
        self.assertEqual( x.get(key), value )

        
        x.execute(key, 'update', update2)
        self.assertEqual( x.get(key), value2 )

        x.execute(key, 'update', update3)
        self.assertEqual( x.get(key), value3 )

        x.execute(key, 'update', update4)
        self.assertEqual( x.get(key), value4 )

    def test_key_pop(self):
        key = 'test_key2'
        value = {'one':1, 'two':2, 'three':3, 'four':4}
        value2 = {'one':1, 'two':2, 'three':3}
        value3 = {'one':1}


        x = JSONDatabase(TestJSONDatabase.state_path, TestJSONDatabase.data_path)
        x.execute(key, "update", value)
        self.assertEqual( x.get(key), value)
        
        x.execute(key, 'pop', ['four'])
        self.assertEqual( x.get(key), value2 )

        x.execute(key, 'pop', ['three', 'two'])
        self.assertEqual( x.get(key), value3 )

    def test_key_drop(self):
        key = 'test-key-3'
        value = {'one':1}
        x = JSONDatabase(TestJSONDatabase.state_path, TestJSONDatabase.data_path)
        x.execute(key, 'update', value)
        self.assertEqual(x.get(key), value)
        x.execute(key, 'drop', None)
        self.assertEqual(x.get(key), None)

if __name__ == '__main__':
    unittest.main()

