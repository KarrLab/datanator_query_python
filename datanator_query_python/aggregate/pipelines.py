"""Various aggregation pipelines
"""


class Pipeline:
    def __init__(self):
        pass

    def aggregate_kegg_orthology(self, expr, projection={'_id': 0, 'gene_ortholog': 0}):
        """Aggregate kegg orthology information
        
        Args:
            expr(:obj:`Obj`): match expression.
            projection(:obj:`Obj`, optional): projection in pipeline.

        Return:
            (:obj:`list`)
        """
        return [{"$match":
                    {"$expr": expr}
                },
                {"$project": projection}]