# Key Value Query Language
# Try to be as much like SQL as possible, sorta.

def digs(s, objs, default=None):
    return map( lambda obj: dig(s, obj, default), objs )

def dig(s, obj, default=None):
    if s.count('.') == 0:
        if type(obj) == dict:
            if s in obj:
                return obj[s]
            else:
                if default:
                    return default
                else:
                    return obj[s]
        elif type(obj) == list and s.isdigit() or (s[0] == '-' and s[1:].isdigit()):
            index = int(s)
            return obj[index]
        else:
            return getattr(obj, s, default)
    else:
        left, rest = s.split('.', 1)
        if type(obj) == dict:
            if left in obj:
                return dig( rest, obj[left] )
            else:
                return default
        else:
            new_obj = getattr(obj, left, default)
            if new_obj:
                return dig( rest, new_obj )
    return default

