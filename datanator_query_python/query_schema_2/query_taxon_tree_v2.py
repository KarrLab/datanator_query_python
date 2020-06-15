from datanator_query_python.config import motor_client_manager
from collections import deque 


class QTaxon:

    def __init__(self,
                collection='taxon_tree',
                db='datanator-test',
                max_entries=float('inf')):
        self.collection = motor_client_manager.client.get_database(db)[collection]

    async def get_canon_ancestor(self, _id, _format='tax_id'):
        """Get organism's canon ancestor information.
        
        Args:
            _id(:obj:`int` or :obj:`str`): identification for organism.
            format(:obj:`str`, optional): identification format, i.e. tax_id or tax_name.

        Return:
            (:obj:`tuple` of :obj:`list` of :obj:`int` and :obj:`list` of :obj:`str`): canon_anc_id, canon_anc_name
        """
        query = {_format: _id}
        doc = await self.collection.find_one(filter=query, 
                                            projection={"canon_anc_names": 1,
                                                        "canon_anc_ids": 1})
        if doc is None:
            return [], []
        else:
            return doc.get("canon_anc_ids", []), doc.get("canon_anc_names", [])

    async def aggregate_distance(self, docs, target_org, _format='tax_id',
                          name_field='species_name'):
        """Aggregate distance information between organisms in documents and target
        organism. 
        e.g. 1) org_0 has ancestors [a, b, c, d, e]; target_org has ancestors 
        [a, b, c, f, g, h]  -> org_0_dist = 2 (e,d), target_org_dist = 3 (h,g,f).
        2) org_0 has ancestors [a, b, c, d, e]; target_org is d -> target_org_dist=0.

        Args:
            docs(:obj:`list` of :obj:`Obj`): list of docs to be compared with target org.
            target_org (:obj:`int` or :obj:`str`): identification of the target organism.
            _format (:obj:`str`, optional): identification format. Defaults to 'tax_id'.
            name_field (:obj:`str`, optional): Field where species name is in each doc. Defaults to 'species_name'.

        Return:
            (:obj:`list` of :obj:`Obj`): Objects containing information on distance to target_org.
        """
        # find target organism document
        target_org_doc = await self.collection.find_one(filter={_format: target_org},
                                                  projection={"canon_anc_ids": 1, "canon_anc_names": 1,
                                                              "tax_name": 1})
        if target_org_doc is None:
            return docs
        else:
            target_org_name = target_org_doc['tax_name']
            target_org_anc_ids = target_org_doc['canon_anc_ids']
            target_org_anc_ids_set = set(target_org_anc_ids)
            target_org_anc_names = target_org_doc['canon_anc_names']
            for doc in docs:
                measured = doc['canon_anc_ids']
                intersect = len(set(measured).intersection(target_org_anc_ids_set))
                measured_dist = len(measured) - intersect
                target_dist = len(target_org_anc_ids) - intersect
                doc['taxon_distance'] = {doc[name_field]: measured_dist, 
                                         target_org_name: target_dist,
                                         "{}_canon_ancestors".format(target_org_name): target_org_anc_names}
            return docs