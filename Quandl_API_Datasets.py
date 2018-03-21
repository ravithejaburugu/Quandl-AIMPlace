# -*- coding: utf-8 -*-
"""
Created on Thu Dec 07 12:23:37 2017

@author: RAVITHEJA
"""

import os
import logging
import time
import requests
import zipfile
import urllib2
import json
from config import mongo_config
from MongodbConnector import mongodbConnector
from os import walk
from datetime import datetime


DEFAULT_DATA_PATH = os.path.abspath(os.path.join(
    os.path.dirname('__file__'), '', 'Quandl'))
mongo = mongodbConnector()


def getCodesInCSVsForAllDatasets(quandl_apikey):
    logging.basicConfig(format='%(asctime)s %(levelname)s \
                        %(module)s.%(funcName)s :: %(message)s',
                        level=logging.INFO)

    q_db_base_url = "https://www.quandl.com/api/v3/databases"
    q_databases_url = q_db_base_url + "?api_key={0}&page={1}"
    q_codes_url = q_db_base_url + "/{0}/codes.json?api_key={1}"

    page = 0
    database_codes = {}
    premium_codes = []
    prev_codes_count = -1
    total_codes = 0
    json_data = {}

    while prev_codes_count != total_codes:
        prev_codes_count = total_codes
        try:
            page += 1
            q_db_URL = q_databases_url.format(quandl_apikey, str(page))

            time.sleep(2)
            json_data = (requests.get(q_db_URL)).json()

            """with open("quandl_datasets.json", 'a+') as ff:
                for d in json_data['databases']:
                    ddata = {}
                    if not d['premium']:
                        database_codes[d['database_code']] = d['name']
                        try:
                            #print d
                            #print str(len(database_codes)) + ',' + d['name'] + ',' + d['database_code'] + ',' + d['description']
                            
                            title = d['name']
                            created_time = datetime.now().strftime("%Y-%m-%d")
                            ddata["status"] = "Active"
                            ddata["subtitle"] = ""
                            ddata["description"] = d['description']
                            ddata["quandlCollectionCode"] = d['database_code']
                            ddata["tags"] = ["Quandl", "FinancialData", title]
                            ddata["price"] = "Contact Sales"
                            ddata["userId"] = "cogadmin"
                            ddata["imageCaption"] = ""
                            ddata["videoLink"] = ""
                            ddata["processCount"] = 1
                            ddata["createdAt"] = created_time
                            ddata["lastUpdatedAt"] = created_time
                            ddata["imageIcon"] = "images/icon-dataset.png"
                            ddata["license"] = "All Rights Reserved"
                            ddata["title"] = title
                            ddata["author"] = ""
                            ddata["tenantId"] = "cogscale"
                            ddata["version"] = "v1"
                            ddata["detailId"] = ""
                            ddata["provider"] = "Quandl"
                            ddata["tutorials"] = ""
                            ddata["scope"] = "public"
                            ddata["type"] = "datasets"
                            ddata["Industry"] = "Financial Services"
                            ddata["subIndustry"] = ""
                            
                            json.dump(ddata, ff)                       
                        except:
                            raise
    
                    if d['premium']:
                        premium_codes.append(d['database_code'])"""
            
            for d in json_data['databases']:
                if d['premium']:
                    premium_codes.append(d['database_code'])
                if not d['premium']:
                    database_codes[d['database_code']] = d['name']
    
            total_codes = len(database_codes) + len(premium_codes)
        except:
            raise

    for code in database_codes.keys():
        zip_filename = code + '-datasets-codes.zip'

        time.sleep(1)
        resp = urllib2.urlopen(q_codes_url.format(code, quandl_apikey))
        zipcontent = resp.read()

        with open(zip_filename, 'wb') as zfw:
            zfw.write(zipcontent)

        with zipfile.ZipFile(zip_filename, "r") as zfr:
            zfr.extractall(DEFAULT_DATA_PATH)

        saveCodesInMongo(database_codes[code])
        os.remove(zip_filename)
    
    logging.info(str(len(database_codes))
                 + " datasets should be downloaded to Mongo")


def saveCodesInMongo(qcode_name):

    quandl_codes_colln_name = mongo_config.get('quandl_codes_colln_name')

    q_data_base_URL = "https://www.quandl.com/api/v3/datasets/{0}"

    filenamesList = []
    for (dirpath, dirnames, filenames) in walk(DEFAULT_DATA_PATH):
        filenamesList.extend(filenames)

    qcodes_colln = mongo.initialize_mongo(quandl_codes_colln_name)
    for fn in filenamesList:
        try:
            dataset_qcodes = []
            logging.info(fn + " extracted.")
            codesFile = os.path.abspath(os.path.join(DEFAULT_DATA_PATH, fn))
            dataset = fn.replace('-datasets-codes.csv', '')
            with open(codesFile, 'r') as csv_file:
                csvlines = csv_file.readlines()

                for num, line in enumerate(csvlines):
                    codeline = line.split(',')
                    if len(codeline) > 1:
                        dataset_code = codeline[0]
                        dataset_descrpn = codeline[1]
                        created_time = datetime.now().strftime("%Y-%m-%d")

                        code_doc = {"dataset": dataset,
                                    "dataset_code": dataset_code,
                                    "description": dataset_descrpn,
                                    "base_url": q_data_base_URL.format(dataset_code),
                                    "created_time":	created_time,
                                    "name": qcode_name,
                                    "_id": dataset_code,
                                    }
                        dataset_qcodes.append(code_doc)

            qcode_cursor = qcodes_colln.find_one({'dataset': dataset})

            if qcode_cursor:
                mongo.bulk_mongo_update(qcodes_colln, dataset_qcodes)
            else:
                mongo.bulk_mongo_insert(qcodes_colln, dataset_qcodes)

        except:
            continue
        os.remove(codesFile)
