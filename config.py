# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 10:07:02 2017

@author: RAVITHEJA
"""

import os

argument_config = {
    'quandl_apikey': os.getenv('QUANDL_APIKEY', 'TciSq6VKzj5uNK7oWDvB'), # o7xFVwAfWTqCsY5mgMGh
    'ckan_host': os.getenv('CKAN_HOST', 'http://40.71.214.191:80'),
    'api_key': os.getenv('CKAN_API_KEY', '8613bf84-7b92-40f3-aa08-056c5f65421b'),
    'publisher': os.getenv('PUBLISHER', 'random trees'),
    'owner_org': os.getenv('OWNER_ORG', 'quandl-fin7'),
    'ckan_private': os.getenv('CKAN_PRIVATE', ''),
}

mongo_config = {
    'mongo_uri': os.getenv('MONGO_URI', 'cluster0-shard-00-00-kjzdb.mongodb.net:27017'),
    #'mongo_uri': os.getenv('MONGO_URI', 'localhost:27017'),
    'ssl_required': os.getenv('MONGO_SSL_REQUIRED', True),
    'requires_auth': os.getenv('REQUIRES_AUTH', 'true'),
    'mongo_username': os.getenv('MONGO_USER', 'admin'),
    'mongo_password': os.getenv('MONGO_PASSWORD', 'R@ndomTrees123'),
    'mongo_auth_source': os.getenv('MONGO_AUTH_SOURCE', 'admin'),
    'mongo_auth_mechanism': os.getenv('MONGO_AUTH_MECHANISM', 'SCRAM-SHA-1'),
    #'mongo_auth_mechanism': os.getenv('MONGO_AUTH_MECHANISM', 'MONGODB-CR'),
    'db_name': os.getenv('MONGO_DB_NAME', 'Quandl2'),
    'mongo_index_name': os.getenv('MONGO_INDEX_NAME', 'csrtc'),
    'meta_colln_name': os.getenv('METADATA_COLLN_NAME', 'metadata'),
    'quandl_codes_colln_name': os.getenv('QUANDL_CODES_COLLN_NAME', 'dataset'),
}
