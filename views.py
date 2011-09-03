import types

def parse_object(obj, path):
    cur = path.split('.')[0]
    if type(obj) == types.DictionaryType:

class View(object):
    def __init__(self, items=None, attr_key=None):
        if items == None:
            self.items = []
        else:
            self.items = items
        self.attr_key = attr_key
            
    def add(self, item):
        self.items.append(item)
        
    def remove(self, item):
        self.items.remove(item)
        
    def filter(self, func):
        return View( filter(func, self.items), attr_key = self.attr_key )

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "<View: %r>" % self.items

    def __iter__(self):
        for i in self.items:
            yield i

    def find(self, d=None):
        if d == None:
            return View(self.items, self.attr_key)
        attr_key = self.attr_key

        def is_match(item):
            for m_key, m_value in d.iteritems():
                if type(item) == types.DictionaryType:
                    def test(x):
                        if attr_key == None:
                            if (m_key in x): 
                                return x[m_key] 
                            else:
                                return None
                        else:
                            data = getattr(x, attr_key)
                            if (m_key in data):
                                return data[m_key]
                            else:
                                return None
                    keyf = test
                elif type(item) in (types.ObjectType, types.InstanceType, types.ClassType) or issubclass(item.__class__, object):
                    if attr_key == None:
                        keyf = lambda x: getattr(item, m_key, False)
                    else:
                        keyf = lambda x: getattr(getattr(x, attr_key), m_key)
                else:
                    raise Exception("I don't know how to handle this type: %r" % type(item))

                if keyf(item) == m_value:
                    return True
                else:
                    return False

        return View( filter( is_match, self.items ), attr_key=self.attr_key )

    def sort(self, key):
        pass

    def limit(self, amount):
        return View( self.items[:amount], self.attr_key )

    def offset(self, amount):
        return View( self.items[amount:], self.attr_key )

    def head(self):
        if len(self.items):
            return self.items[0]
        else:
            return None

    def tail(self):
        return View(self.items[1:], self.attr_key)

if __name__ == '__main__':
    x = View([1,2,3,4,5])
    print x.filter( lambda x: True if x > 3 else False )

    y = View()
    for i in range(0, 10):
        y.items.append( {'title':'number %i' % i, 'count':i} )
        y.items.append( {'title':'number %i' % i, 'count':i} )
        y.items.append( {'title':'number %i' % i, 'count':i} )

    print y.find({'count': 0})

    class Test(object):
        def __init__(self, count=0):
            self.count = count

    z = View()
    for i in range(0, 10):
        z.items.append(Test(i))
        z.items.append( {'title':'number %i' % i, 'count':i} )

    for i in z.find({'count':0}):
        print i

    print y.find({'count':0}).head()

    class ComplexTest(object):
        def __init__(self, data):
            self.data = data

    z = View(attr_key='data')
    for i in range(0, 10):
        z.add( ComplexTest( {str(i):i} ) )
    print z.find( {'1':1} )
    
