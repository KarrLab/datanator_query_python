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
import itertools
from collections import defaultdict
import json


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
    sabio_docs = src.taxon_dist("sabio_rk_old", "taxon_id", match={"$match": {"taxon_id": {"$ne": None}}})
    rnamod_docs = src.taxon_dist("rna_modification", "modifications.ncbi_taxonomy_id", unwind={"$unwind": "$modifications"}, 
                                 match={"$match": {"modifications.ncbi_taxonomy_id": {"$ne": None}}})
    rnahalf_docs = src.taxon_dist("rna_halflife_new", "halflives.ncbi_taxonomy_id", unwind={"$unwind": "$halflives"})
    conc_docs = src.taxon_dist("metabolite_concentrations", "concentrations.ncbi_taxonomy_id", unwind={"$unwind": "$concentrations"})

    obj = defaultdict(int)
    for (p_doc, s_doc, m_doc, l_doc, c_doc) in itertools.zip_longest(uniprot_docs, sabio_docs, rnamod_docs, rnahalf_docs, conc_docs, fillvalue={'_id': 1, 'count': 0}):
        obj[p_doc['_id']] += p_doc['count']
        obj[s_doc['_id']] += s_doc['count']
        obj[m_doc['_id']] += m_doc['count']
        obj[l_doc['_id']] += l_doc['count']
        obj[c_doc['_id']] += c_doc['count']

    # sort obj
    new_obj = {k: v for k, v in sorted(obj.items(), key=lambda item: item[1], reverse=True)}

    with open("./docs/taxon_distribution.json", "w+") as outfile: 
        json.dump(new_obj, outfile)

    final_obj = {"others": 0}
    for i, (key, val) in enumerate(new_obj.items()):
        if i < 11 and key != 1:
            name = src.db_obj['taxon_tree'].find_one({"tax_id": int(key)}, projection={"tax_name": 1})["tax_name"]
            final_obj[name] = val
        else:
            final_obj["others"] += val 

    with open("./docs/taxon_distribution_frontend.json", "w+") as outfile: 
        json.dump(final_obj, outfile)


if __name__ == '__main__':
    main()