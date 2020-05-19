"""Various aggregation pipelines
"""
from datanator_query_python.aggregate import lookups


class Pipeline:
    def __init__(self):
        self.lookup_manager = lookups.Lookups()

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

    def aggregate_common_canon_ancestors(self, org0, org1, org_format='tax_id'):
        """Get common canonical ancestors between two organisms.

        Args:
            org0(:obj:`str` or :obj:`int`): organism 0.
            org1(:obj:`str` or :obj:`int`): organism 1.
            org_format(:obj:`str`, optional): field used to identify organism (tax_id or tax_name).

        Return:
            (:obj:`list`)
        """
        return

    def aggregate_taxon_distance(self, match, local_field, _as):
        """Aggrate canonical taxon distance information for frontend
        (avoiding iteration)

        Args:
            match(:obj:`Obj`): match object in pipeline.
            local_field(:obj:`str`): field from input documents.
            _as(:obj:`str`): output array

        Return:
            (:obj:`list`)
        """
        lookup_obj = self.lookup_manager.simple_lookup("taxon_tree", local_field, "tax_id", _as)
        return