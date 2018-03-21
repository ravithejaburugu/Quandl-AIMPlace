# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 10:07:02 2017

@author: RAVITHEJA
"""

import logging
from config import mongo_config
from ckanForMetadata import insert_into_ckan


def persistFinData(mongo, mongo_colln, source, json_data, data, dataset_code,
                   description, meta_updated, data_mode, qcode_name):
    """Collects the Quandl JSON response data and inserts into mongo collection
    and updates CKAN."""

    logging.basicConfig(format='%(asctime)s %(levelname)s \
                        %(module)s.%(funcName)s :: %(message)s',
                        level=logging.INFO)

    # fetching arguments from config.
    mongo_uri = mongo_config.get('mongo_uri')
    meta_col_name = mongo_config.get('meta_colln_name')

    try:
        mongo.bulk_mongo_insert(mongo_colln, data)

        # logging.info(data_mode)
        # logging.info(meta_updated)
        if data_mode == "initial" and meta_updated:
                # METADATA Collection
                meta_mongo_colln = mongo.initialize_mongo(meta_col_name)
                meta_feedObj = json_data["dataset"]
                meta_feedObj['_id'] = source + "." + dataset_code.split("/")[1]
                mongo.insert_into_mongo(meta_mongo_colln, meta_feedObj)

                # CKAN
                refresh_rate = json_data["dataset"]["frequency"]
                insert_into_ckan(mongo_uri, source, qcode_name, description,
                                 refresh_rate)
    except:
        raise
        logging.error("Error while Initializing Mongo.")

    return meta_updated
