import json

class Statement(object):
    def __init__(self, target_key, data_hash, command, args):
        self.target_key = target_key
        self.data_hash = data_hash
        self.command = command
        self.args = args

    def __repr__(self):
        return self.as_json()

    def as_json(self):
        return json.dumps( [self.target_key, self.data_hash, self.command, self.args] )

    @classmethod
    def from_json(self, s):
        return Statement(*json.loads(s))
