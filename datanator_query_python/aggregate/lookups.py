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
            foreign_field(:obj:`str`): field from the documents of the "_from" collection.
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

    def complex_lookup(self, _from, let, pipeline, _as):
        """Complex lookup object (lookups with let).

        Args:
            _from (:obj:`str`): collection to join.
            let (:obj:`Obj`): specifies variables to use in pipeline stages.
            pipeline (:obj:`list` of :obj:`Obj`): specifies the pipeline to run on the joined collection.
            _as (:obj:`str`): output array field.
        """
        return  {"$lookup":
                    {
                        "from": _from,
                        "let": let,
                        "pipeline": pipeline,
                        "as": _as
                    }
                }