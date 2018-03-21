# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 10:07:02 2017

@author: RAVITHEJA
"""

import time
import logging
import json
import os
import requests as req
from config import argument_config, mongo_config
from Quandl_API_Datasets import getCodesInCSVsForAllDatasets
from FinDataPersist import persistFinData
from MongodbConnector import mongodbConnector
from datetime import datetime, timedelta

try:
    mongo = mongodbConnector()
except:
    raise

def saveQuandlData(resp_data, src_colln, src_colln_name, dataset_descrpn,
                   dataset_code, data_mode, prev_count, qcode_name):
    try:
        # Need JSON format to save in Mongo
        json_data = json.loads(resp_data)

        if "dataset" not in json_data \
                or len(json_data["dataset"]["data"]) == 0:
            return

        # Parse the 'data' column-wise
        parsed_json_data = parseDataColumns(
                json_data["dataset"]["column_names"],
                json_data["dataset"]["data"])

        logging.info(str(len(parsed_json_data)) + " new records in "
                     + dataset_code)

        if len(parsed_json_data) == 0:
            return

        meta_updated = True
        del json_data["dataset"]["data"]

        # Add the 'created_time' field for every record.
        json_data["dataset"]["created_time"] = datetime.now()\
                                               .strftime("%Y-%m-%d %H:%M:%S")

        code_part = dataset_code.split("/")[1]
        for sno, data in enumerate(parsed_json_data, start=1):
            data["created_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data["dataset_code"] = dataset_code
            data["_id"] = code_part + "_" + str(sno) + str(prev_count)
            data["provider"] = "Quandl"
            data["title"] = dataset_code
            data["description"] = dataset_code
            data["tags"] = ["Quandl", src_colln_name, code_part, qcode_name]

        persistFinData(mongo, src_colln, src_colln_name, json_data,
                       parsed_json_data, dataset_code, dataset_descrpn,
                       meta_updated, data_mode, qcode_name)
    except:
        pass


def parseDataColumns(column_names, json_data_part):
    """Parses the json data from list of elements to the list of dicts with
    keys as in column_names and values of each record of Data from dataset."""

    json_data_parsed = []
    column_names = [c.replace('.', '') for c in column_names]
    for rec in json_data_part:
        json_data_parsed.append({column_names[c]: rec[c]
                                for c in range(len(column_names))})
    return json_data_parsed


def main():
    """Initiates the Financial news extraction from Quandl using API calls."""

    t1 = time.time()
    logging.basicConfig(format='%(asctime)s %(levelname)s \
                        %(module)s.%(funcName)s :: %(message)s',
                        level=logging.INFO)

    # fetching arguments from config.
    quandl_apikey = argument_config.get('quandl_apikey')
    meta_col_name = mongo_config.get('meta_colln_name')
    quandl_codes_colln_name = mongo_config.get('quandl_codes_colln_name')

    qcodes_colln = mongo.initialize_mongo(quandl_codes_colln_name)
    meta_mongo_colln = mongo.initialize_mongo(meta_col_name)

    # Executes code uninterrupted.
    while True:
        try:
            # Fetching dataset codes from Quandl
            qcodes_cursor = qcodes_colln.find()
            for qcur in qcodes_cursor:
                # Redownload the codes for every 30 days
                curr_date = datetime.now().strftime("%Y-%m-%d")
                codes_dt = datetime(*map(int, (qcur['created_time'])\
                                              .split("-")))
                curr_dt = datetime(*map(int, curr_date.split("-")))
    
                if (curr_dt - codes_dt).days > 30:
                    getCodesInCSVsForAllDatasets(quandl_apikey)
                break
            else:
                # Downloading the Quandl codes for the first time.
                getCodesInCSVsForAllDatasets(quandl_apikey)
    
            # Fetch the Quandl codes from mongo collection to extract data,
            qcodes_cursor = qcodes_colln.find()
    
            src_colln_list = []
            for qcur in qcodes_cursor:
                base_url = qcur['base_url']
                data_URL = base_url + "?api_key={0}"
                dataset_code = qcur['dataset_code']
                dataset_descrpn = qcur['description']
                qcode_name = qcur['name']
    
                src_colln_name = dataset_code.lower().split("/")[0]
                meta_obj_name = src_colln_name + "." + dataset_code.split("/")[1]
    
                if src_colln_name not in src_colln_list:
                    src_colln_list.append(src_colln_name)
                else:
                    continue
                logging.info("Executing dataset code :: " + dataset_code)
    
    
                src_colln = mongo.initialize_mongo(src_colln_name)
                resp_data = ''
                mongo_id = ''
                data_mode = ''
                prev_count = 0
    
                # Check if Collection already exists in MongoDB.
                metadata_count = src_colln.count()
                if metadata_count == 0:
    
                    time.sleep(3)
                    resp = os.popen("curl " + data_URL.format(quandl_apikey))
                    resp_data = resp.read()
                    data_mode = "initial"
        
                    # Persisting functionality to Mongo.
                    saveQuandlData(resp_data, src_colln, src_colln_name,
                                   dataset_descrpn, dataset_code, data_mode,
                                   prev_count, qcode_name)
        except:
            raise

    logging.info("Total time taken to fetch data from Quandl : " +
                 str(round(float((time.time() - t1)/60), 1)) + " minutes")


if __name__ == '__main__':
    main()
