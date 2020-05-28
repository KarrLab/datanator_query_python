"""Tabulate taxonomic distribution of all observations across relevant collections.
https://github.com/KarrLab/datanator_rest_api/issues/97
"""
from datanator_query_python.util import mongo_util
from datanator_query_python.aggregate import pipelines


class Tabu(mongo_util.MongoUtil):
    def __init__(self, MongoDB=None, db=None, username=None, password=None,
                authSource=None, readPreference=None, max_entries=float('inf'),
                verbose=True):
        super().__init__(MongoDB=MongoDB, db=db, username=username, password=password,
                         authSource=authSource, readPreference=readPreference,
                         max_entries=max_entries, verbose=verbose)
        self.verbose = verbose
        self.max_entries = max_entries
        self.uniprot_col = self.db_obj['uniprot']
        self.sabiork_col = self.db_obj['sabio_rk_old']
        self.rna_half_col = self.db_obj['rna_halflife_new']
        self.rna_mod_col = self.db_obj['rna_modification']
        self.meta_con_col = self.db_obj['metabolite_concentrations']
        self.pipeline_manager = pipelines.Pipeline()

    def uniprot_taxon_dist(self):
        """Tabulate uniprot collections taxonomic distribution.

        Return:
            (:obj:`CommandCursor`)
        """
        match = {"$match": {"abundances": {"$exists": True}}}
        pipeline = self.pipeline_manager.aggregate_all_occurences("ncbi_taxonomy_id", match=match,
                                                                  project_post={"count": 1, "species_name": 1})
        return self.uniprot_col.aggregate(pipeline)

    def taxon_dist(self, collection, field, match=None, unwind=None):
        """Generalized version of uniprot_taxon_dist.

        Args:
            collection(:obj:`str`): name of collection.
            field(:obj:`str`): Field upon which aggregation will be done.
            match(:obj:`Obj`): Filtering of unnecessary data.
            unwind(:obj:`Obj`): Unwind operation if field of interest is in subdocuments.

        Return:
            (:obj:`CommandCursor`)
        """
        pipeline = self.pipeline_manager.aggregate_all_occurences(field, match=match,
                                                                  unwind=unwind)
        return self.db_obj[collection].aggregate(pipeline)


from datanator_query_python.config import config

def main():
    db = 'datanator'
    conf = config.TestConfig()
    username = conf.MONGO_TEST_USERNAME
    password = conf.MONGO_TEST_PASSWORD
    MongoDB = conf.SERVER
    src = Tabu(MongoDB=MongoDB, username=username, password=password,
               db=db, verbose=True, authSource='admin',
               readPreference='nearest', max_entries=10)

    uniprot_docs = src.uniprot_taxon_dist()