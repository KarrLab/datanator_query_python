"""Form aggregate lookup objects
"""


class Lookups:
    def __init__(self):
        pass

    def simple_lookup(self, _from, local_field, foreign_field, _as):
        """Simple look up operation (without `let` or `pipeline`).

        Args:
            _from(:obj:`str`): collection to join.
            local_field(:obj:`str`): field from input documents.
            foreign_field(:obj:`str`): field from kegg_orthology collection.
            _as(:obj:`str`): output array field.

        Return:
            (:obj:`Obj`)
        """
        return  {"$lookup":
                    {
                        "from": _from,
                        "localField": local_field,
                        "foreignField": foreign_field,
                        "as": _as
                    }
                }