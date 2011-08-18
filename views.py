import types

class View(object):
    def __init__(self, items=None):
        if items == None:
            self.items = []
        else:
            self.items = items
            
    def add(self, item):
        self.items.append(item)
        
    def remove(self, item):
        self.items.remove(item)
        
    def filter(self, func):
        return View( filter(func, self.items) )

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "<View: %r>" % self.items

    def __iter__(self):
        for i in self.items:
            yield i

    def l_find(self, l):
        return View( filter( l, self.items ) )

    def find(self, d=None):
        if d == None:
            return View(self.items)

        def is_match(item):
            for m_key, m_value in d.iteritems():
                if type(item) == types.DictionaryType:
                    def test(x):
                        if (m_key in x): 
                            return x[m_key] 
                        else:
                            return None
                    keyf = test
                elif type(item) in (types.ObjectType, types.InstanceType, types.ClassType) or issubclass(item.__class__, object):
                    keyf = lambda x: getattr(item, m_key, False)
                else:
                    raise Exception("I don't know how to handle this type: %r" % type(item))

                if keyf(item) == m_value:
                    return True
                else:
                    return False

        return View( filter( is_match, self.items ) )

    def sort(self, key):
        pass

    def limit(self, amount):
        return View( self.items[:amount] )

    def offset(self, amount):
        return View( self.items[amount:] )

    def first(self):
        if len(self.items):
            return self.items[0]
        else:
            return None

    def rest(self):
        return View(self.items[1:])

if __name__ == '__main__':
    x = View([1,2,3,4,5])
    print x.l_find( lambda x: True if x > 3 else False )

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

    print y.find({'count':0}).first()
