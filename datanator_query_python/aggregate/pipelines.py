"""Various aggregation pipelines
"""


class Pipeline:
    def __init__(self):
        pass

    def aggregate_kegg_orthology(self):
        """Aggregate kegg orthology information

        Return:
            (:obj:`list`)
        """
        return 
        [{"$match":
            {"$expr":
                {"$and":
                    [
                        {"$eq": [ "$stock_item",  "$$order_item" ] },
                        {"$gte": [ "$instock", "$$order_qty" ] }
                    ]
                }
            }
        },
        {"$project": {"stock_item": 0, "_id": 0}}]