# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 10:07:02 2017

@author: RAVITHEJA
"""

import logging
import ckanapi
from datetime import datetime
from config import argument_config, mongo_config


def insert_into_ckan(mongo_uri, source, qcode_name, description, refresh_rate):
    """"CKAN holds the meta information about the saved data of MongoDB."""

    logging.basicConfig(format='%(asctime)s %(levelname)s \
                        %(module)s.%(funcName)s :: %(message)s',
                        level=logging.INFO)

    # Fetch config params.
    ckan_host = argument_config.get('ckan_host')
    api_key = argument_config.get('api_key')
    owner_org = argument_config.get('owner_org')
    publisher = argument_config.get('publisher')
    ckan_private = argument_config.get('ckan_private')
    db_name = mongo_config.get('db_name')

    ckan_ckan = ckanapi.RemoteCKAN(ckan_host, apikey=api_key)

    package_name = source.lower().replace("_", "-")\
                                 .replace("(", "-")\
                                 .replace(")", "-")\
                                 .replace("/", "-")\
                                 .replace(".", "")\
                                 .replace("&", "")\
                                 .replace(":", "")\
                                 .replace("---", "-")\
                                 .replace("--", "-")

    package_name = package_name[:99]
    if package_name.endswith("-"):
        package_name = package_name.rstrip('-')

    # package_title = source.replace("_", " ")
    package_title = qcode_name

    dict_additional_fields = {
            'Title': package_title,
            'Sourcing date': datetime.now().strftime("%B %d, %Y, %H:%M"),
            'Source': source,
            'Datastore': mongo_uri,
            'Database name': db_name,
            'Collection': source,
            'Description': description,
            'Refresh rate': refresh_rate,
            }
    additional_fields = []
    for k, v in dict_additional_fields.items():
        additional_fields.append({'key': k, 'value': v})

    tags = buildTags(package_name)
    try:
        ckan_ckan.action.package_create(name=package_name,
                                        title=package_title,
                                        maintainer=publisher,
                                        tags=tags,
                                        notes=description,
                                        private=ckan_private,
                                        owner_org=owner_org,
                                        extras=additional_fields,
                                        )
    except:
        try:
            ckan_ckan.action.package_update(id=package_name,
                                            title=package_title,
                                            maintainer=publisher,
                                            tags=tags,
                                            notes=description,
                                            private=ckan_private,
                                            owner_org=owner_org,
                                            extras=additional_fields,
                                            )
        except:
            logging.error("CKAN package creation/updation failed: "
                          + package_name)


def buildTags(source):
    """Tags need to be customized and built separately."""
    tags = []
    tags.append({'name': source.replace("-", " ")})
    tags.append({'name': "Quandl"})
    tags.append({'name': "FinancialData"})
    return tags
