import unittest
import os
import sys
sys.path.append('.')

from adapters.json_files import *

class TestJSONDatabase(unittest.TestCase):
    data_path = '/tmp/json_test/'
    state_path = '/tmp/json_state.db'

    def init(self):
        if os.path.isdir(TestJSONDatabase.data_path):
            os.removedirs(TestJSONDatabase.data_path)
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

if __name__ == '__main__':
    unittest.main()

