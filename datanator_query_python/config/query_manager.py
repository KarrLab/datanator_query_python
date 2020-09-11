from datanator_query_python.config import config
from datanator_query_python.query import (query_protein, query_metabolites,
                                         query_sabiork_old, query_taxon_tree, full_text_search,
                                         query_uniprot, query_rna_halflife, query_kegg_orthology,
                                         query_metabolites_meta, query_metabolite_concentrations)

class Manager:

    def __init__(self):
        self.username = config.AtlasConfig.USERNAME
        self.password = config.AtlasConfig.PASSWORD
        self.server = config.AtlasConfig.SERVER
        self.authDB = config.AtlasConfig.AUTHDB
        self.read_preference = config.AtlasConfig.READ_PREFERENCE
        self.repl = config.AtlasConfig.REPLSET

    def protein_manager(self, database="datanator"):
        return query_protein.QueryProtein(username=self.username, password=self.password, server=self.server,
        authSource=self.authDB, readPreference=self.read_preference, replicaSet=self.repl, database=database)

    def metabolite_concentration_manager(self):
        return query_metabolite_concentrations.QueryMetaboliteConcentrations(MongoDB=self.server, db='datanator',
        collection_str='metabolite_concentrations', username=self.username, password=self.password, authSource=self.authDB,
        readPreference=self.read_preference, replicaSet=self.repl)

    def eymdb_manager(self):
        return query_metabolites.QueryMetabolites(
            username=self.username,
            password=self.password,
            MongoDB=self.server,
            authSource=self.authDB,
            db='datanator',
            readPreference=self.read_preference,
            replicaSet=self.repl)


class RxnManager:

    def rxn_manager(self):
        return query_sabiork_old.QuerySabioOld(username=config.AtlasConfig.USERNAME, 
        password=config.AtlasConfig.PASSWORD, MongoDB=config.AtlasConfig.SERVER,
        authSource=config.AtlasConfig.AUTHDB, readPreference=config.AtlasConfig.READ_PREFERENCE,
        replicaSet=config.AtlasConfig.REPLSET)


class TaxonManager:

    def txn_manager(self):
        return query_taxon_tree.QueryTaxonTree(username=config.AtlasConfig.USERNAME, 
        password=config.AtlasConfig.PASSWORD, MongoDB=config.AtlasConfig.SERVER,
        authSource=config.AtlasConfig.AUTHDB, readPreference=config.AtlasConfig.READ_PREFERENCE,
        replicaSet=config.AtlasConfig.REPLSET)


class FtxManager:

    def ftx_manager(self):
        return full_text_search.FTX(profile_name=config.FtxConfig.REST_FTX_AWS_PROFILE)


def uniprot_manager():
    return query_uniprot.QueryUniprot(username=config.AtlasConfig.USERNAME, password=config.AtlasConfig.PASSWORD,
    server=config.AtlasConfig.SERVER, authSource=config.AtlasConfig.AUTHDB, readPreference=config.AtlasConfig.READ_PREFERENCE,
    collection_str='uniprot', replicaSet=config.AtlasConfig.REPLSET)

def metabolites_meta_manager():
    return query_metabolites_meta.QueryMetabolitesMeta(MongoDB=config.AtlasConfig.SERVER, db='datanator', username=config.AtlasConfig.USERNAME,
    password=config.AtlasConfig.PASSWORD, authSource=config.AtlasConfig.AUTHDB, readPreference=config.AtlasConfig.READ_PREFERENCE,
    replicaSet=config.AtlasConfig.REPLSET)


class RnaManager:

    def rna_manager(self):
        return query_rna_halflife.QueryRNA(username=config.AtlasConfig.USERNAME, password=config.AtlasConfig.PASSWORD,
        server=config.AtlasConfig.SERVER, authDB=config.AtlasConfig.AUTHDB, readPreference=config.AtlasConfig.READ_PREFERENCE,
        db='datanator', collection_str='rna_halflife_new', replicaSet=config.AtlasConfig.REPLSET)


class KeggManager:

    def kegg_manager(self):
        return query_kegg_orthology.QueryKO(username=config.AtlasConfig.USERNAME, password=config.AtlasConfig.PASSWORD,
        server=config.AtlasConfig.SERVER, authSource=config.AtlasConfig.AUTHDB, readPreference=config.AtlasConfig.READ_PREFERENCE,
        verbose=False, replicaSet=config.AtlasConfig.REPLSET)