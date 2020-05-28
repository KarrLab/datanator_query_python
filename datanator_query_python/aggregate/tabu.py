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

    def uniprot_taxon_dist(self, field):
        """Tabulate uniprot collections taxonomic distribution

        Args:
            field(:obj:`str`): name of field where taxon info is.

        Return:
            (:obj:`CommandCursor`)
        """
        pass