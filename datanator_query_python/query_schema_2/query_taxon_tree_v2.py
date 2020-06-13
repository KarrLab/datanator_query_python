from datanator_query_python.config import query_schema_2_manager, config


class QTaxon(query_schema_2_manager.QM):

    def __init__(self,
                username=config.Config.USERNAME,
                password=config.Config.PASSWORD,
                readPreference=config.Config.READ_PREFERENCE,
                collection='taxon_tree',
                db='datanator-test',
                max_entries=float('inf')):
        super().__init__(username=username, password=password)
        self.collection = self.client.get_database(db,
                                                   read_preference=self.read_preference)[collection]

    def get_canon_ancestor(self, _id, _format='tax_id'):
        """Get organism's canon ancestor information.
        
        Args:
            _id(:obj:`int` or :obj:`str`): identification for organism.
            format(:obj:`str`, optional): identification format, i.e. tax_id or tax_name.

        Return:
            (:obj:`tuple` of :obj:`list` of :obj:`int` and :obj:`list` of :obj:`str`): canon_anc_id, canon_anc_name
        """
        query = {_format: _id}
        doc = self.collection.find_one(filter=query, 
                                 projection={"canon_anc_names": 1,
                                             "canon_anc_ids": 1})
        if doc is None:
            return [], []
        else:
            return doc.get("canon_anc_ids", []), doc.get("canon_anc_names", [])
