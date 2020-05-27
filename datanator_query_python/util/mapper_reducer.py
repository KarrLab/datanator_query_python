"""Form mapper and/or reducer
https://api.mongodb.com/python/current/examples/aggregation.html
"""
from bson.code import Code


class MaRe:
    def __init__(self):
        pass

    def snippet(self, _input):
        """Takes in JavaScript string and code to function.

        Args:
            _input(:obj:`str`): JavaScript string.

        Return:
            (:obj:`bson.code.Code`)
        """
        return Code(_input)