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

    def aggregate_common_canon_ancestors(self, anchor, target, org_format='tax_id',
                                         intersect_name='anc_match'):
        """Get common canonical ancestors between two organisms.

        Args:
            anchor(:obj:`Obj`): document of anchor organism.
            target(:obj:`str` or :obj:`int`): target organism.
            org_format(:obj:`str`, optional): field used to identify organism (tax_id or tax_name).
            intersect_name(:obj:`str`): name for intersection array.

        Return:
            (:obj:`list`)
        """
        suffix = org_format.split('_')[1]
        return  [{"$match": 
                    {org_format: target}
                },
                {"$project": 
                    {intersect_name: 
                        {
                            "$setIntersection": [
                                "$canon_anc_{}s".format(suffix),
                                anchor['canon_anc_{}s'.format(suffix)]
                            ]
                        }
                    }
                }]

    def aggregate_taxon_distance(self, match, local_field, _as, 
                                anchor, target, org_format='tax_id',
                                intersect_name='anc_match'):
        """Aggrate canonical taxon distance information for frontend
        (avoiding iteration)

        Args:
            match(:obj:`Obj`): match object in pipeline.
            local_field(:obj:`str`): field from input documents.
            _as(:obj:`str`): output array.
            anchor(:obj:`Obj`): document of anchor organism.
            target(:obj:`str` or :obj:`int`): target organism.
            org_format(:obj:`str`, optional): field used to identify organism (tax_id or tax_name).
            intersect_name(:obj:`str`): name for intersection array.

        Return:
            (:obj:`list`)
        """
        inner_pipeline = self.aggregate_common_canon_ancestors(anchor, target, org_format=org_format,
                                                               intersect_name=intersect_name)
        return

    def aggregate_total_array_length(self, field):
        """Aggregate the total length of an array field in collection.
        e.g. [{"field": [0, 1]}, {"field": [2]}]

        Args:
            field(:obj:`str`): Name of the field.

        Return:
            (:obj:`list`)
        """
        project = {"$project": {"_len": {"$size": {"$ifNull": ["${}".format(field), []]}}}}
        group = {"$group": {"_id": "$forSum",
                            "total": {"$sum": "$_len"},
                            "count": {"$sum": 1}}}
        return [project, group]

    def aggregate_field_count(self, field, projection={"parameter": 1},
                              match={"parameter.observed_name": "Ki"},
                              unwind=None, group={"$group": {"count": {"$sum": 1}}}):
        """Aggregate number of occurences of a value in field.

        Args:
            field(:obj:`str`): field of interest.
            projection(:obj:`Obj`): Projection (prune unnecessary data in document).
            match(:obj:`Obj`): Further filtering of data that meet certain conditions.

        Return:
            (:obj:`list`)
        """
        result = []
        if projection is not None:
            result.append({"$project": projection})
        if match is not None:
            result.append({"$match": match})
        if unwind is not None:
            result.append(unwind)
        if group is not None:
            group["$group"]["_id"] = "$.{}".format(field)
            result.append(group)
        return result

    def aggregate_all_occurences(self, field):
        """Aggregate all occurences of values in field.

        Args:
            field(:obj:`str`): Name of the field.

        Return:
            (:obj:`list`)
        """
        return  [{'$group': { '_id' : '${}'.format(field), 'count' : {'$sum' : 1}}},
                 {"$project": { 
                    "count": 1
                 }},
                 {"$sort": {"count": 1 }}
                ]