import unittest
from kvql import dig, digs

class TestDigPy(unittest.TestCase):
    def test_basic_dig_dictionary(self):
        class Test():
            def __init__(self):
                self.data = {'one':1}
        
        x = Test()
        result = dig('data.one', x)

        self.assertEqual( result, 1 )

    def test_basic_dig_object(self):
        class Test():
            def __init__(self):
                self.data = [1,2,3]

        x = Test()
        result = dig('x.data', locals())
        
        self.assertEqual( result, [1,2,3] )

    def test_basic_digs_object(self):
        class Test():
            def __init__(self):
                self.data = {'one':1}

        x = Test()
        y = Test()
        z = Test()
        result = digs('data.one', [x,y,z])

        self.assertEqual( result, [1,1,1] )

    def test_list_index(self):
        d = {'list':[1,2,3]}
        result = dig('list.0', d)
        self.assertEqual(result, 1)

        result = dig('list.-1', d)

        self.assertEqual(result, 3)
        
if __name__ == '__main__':
    unittest.main()
