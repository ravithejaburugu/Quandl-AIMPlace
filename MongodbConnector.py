# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 09:00:10 2017

@author: RAVITHEJA
"""

import logging
from pymongo import MongoClient
from config import mongo_config
from uuid import uuid1


class mongodbConnector():

    def __init__(self):
        mongo_uri = mongo_config.get('mongo_uri')
        ssl_required = mongo_config.get('ssl_required')
        requires_auth = mongo_config.get('requires_auth')
        mongo_username = mongo_config.get('mongo_username')
        mongo_password = mongo_config.get('mongo_password')
        mongo_auth_source = mongo_config.get('mongo_auth_source')
        mongo_auth_mech = mongo_config.get('mongo_auth_mechanism')
        db_name = mongo_config.get('db_name')
        self.mongo_index_name = mongo_config.get('mongo_index_name')

        try:
            # Instantiating MongoClient
            client = MongoClient(mongo_uri, ssl=ssl_required,
                                 replicaSet='Cluster0-shard-0')

            # Authenticate MongoDB (Optional)
            if requires_auth == 'true':
                client.the_database.authenticate(mongo_username,
                                                 mongo_password,
                                                 source=mongo_auth_source,
                                                 mechanism=mongo_auth_mech
                                                 )
            self.mongo_inst = client[db_name]
        except IOError:
            raise
            logging.error("Could not connect to Mongo Server")

    def validateCollnIndex(self, mongo_colln, collection_name):
        try:
            # Testing Index with Unique element, to avoid failure of
            # Index creation, in case of Collection does not exist.
            test_uuid = str(uuid1())
            try:
                mongo_colln.insert_one({'uuid': test_uuid})
                mongo_colln.delete_one({'uuid': test_uuid})
            except:
                logging.debug("Collection %s already exists" % collection_name)

            # Create index, if it is not available.
            if self.mongo_index_name not in mongo_colln.index_information():
                mongo_colln.create_index(self.mongo_index_name,
                                         unique=False)
        except:
            raise
            logging.error("Could not create Collection/Index.")

    def initialize_mongo(self, collection_name):
        """Initializes MongoDB Connection and
        returns Mongo instance and MongoCollection for the given Index."""

        try:
            mongo_colln = self.mongo_inst[collection_name]

            self.validateCollnIndex(mongo_colln, collection_name)
        except IOError:
            raise
            logging.error("Could not connect to Mongo Server")

        return mongo_colln

    def insert_into_mongo(self, mongo_colln, feed_object):
        """To insert given JSON data into MongoDB."""
        try:
            mongo_colln.insert_one(feed_object)
        except:
            logging.error("Mongo Insert Exception.")
        finally:
            feed_object.clear
        return True

    def bulk_mongo_insert(self, mongo_colln, obj_list):
        """To insert given List of JSON data into MongoDB in ONE operation."""
        try:
            mongo_colln.insert_many(obj_list)
        except:
            raise
            logging.error("Mongo BULK Insert Exception.")
        return True

    def bulk_mongo_update(self, mongo_colln, obj_list):
        """To update existing data into MongoDB."""
        try:
            for new_obj in obj_list:
                mongo_colln.update_one({'_id': new_obj['_id']},
                                       {"$set": new_obj},
                                       upsert=True)
        except:
            raise
            logging.error("Mongo BULK UPDATE Exception.")
        return True
