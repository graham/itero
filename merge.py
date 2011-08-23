mergers = {
    type({}):merge_dict,
    type([]):merge_list,
    type(1):merge_number,
    type(1.1):merge_number,
    type(set()):merge_set,
    type(""):merge_string
}


def merge_dict(prev, statements):
    pass

def merge_list(prev, statements):
    pass

def merge_number(prev, statements):
    pass

def merge_set(prev, statements):
    pass

def merge_string(prev, statements):
    pass
