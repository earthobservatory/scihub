#!/usr/bin/env python
from __future__ import print_function

import json
import re
import argparse
import datetime
import os
import sys 
import requests
import logging
import urllib
from requests.auth import HTTPBasicAuth
from shapely.geometry.polygon import Polygon
from urlparse import urljoin

import qquery.query
import xml.dom.minidom

allowable_products = ["S1_IW_SLC", "S1_GRD"] #searchable product types
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('scihub_opensearch_query')


'''
This worker queries slc products from scihub copernicus
'''

#Global constants
DEFAULT_DNS = "https://scihub.copernicus.eu/"
SEARCH_PATH = "apihub/search?"
DOWNLOAD_PATH = "apihub/odata/v1/Products('{}')/$value"
BACKUP_URL = "https://tmphub.copernicus.eu/apihub/search?"
dtreg = re.compile(r'S1\w_.+?_(\d{4})(\d{2})(\d{2})T.*')
S1_QUERY_TEMPLATE = 'IW AND producttype:SLC AND platformname:Sentinel-1 AND ' + \
                 'ingestiondate:[{0} TO {1}] AND ' + \
                 'footprint:"Intersects(POLYGON(({2})))"'
GRD_QUERY_TEMPLATE = 'producttype:GRD AND ingestiondate:[{0} TO {1}]'


class SciHubOpenSearch(qquery.query.AbstractQuery):
    '''
    Sci-hub query implementer
    '''
    def query(self,start,end,aoi,dns_alias,mapping='S1_IW_SLC'):
        '''
        Performs the actual query, and returns a list of (title, url) tuples
        @param start - start time stamp
        @param end - end time stamp
        @param aoi - area of interest
        @mapping - type of product to queried. defaults to S1_IW_SLC
        @return: list of (title, url) pairs
        '''
        print("###We came here")
        session = requests.session()
        if mapping == "S1_IW_SLC":
            polygon = ",".join(["%s %s" % (i[0], i[1]) for i in aoi["location"]["coordinates"][0]])
            qur = S1_QUERY_TEMPLATE.format("%sZ" % start,"%sZ" % end, polygon)
            return self.listAll(session,qur,aoi["location"]["coordinates"],dns_alias)
        elif mapping == "GRD":
            qur = GRD_QUERY_TEMPLATE.format(polygon,"%sZ" % start,"%sZ" % end)
            return self.listAll(session,qur,dns_alias=dns_alias)


    @staticmethod
    def getDataDateFromTitle(title):
        '''
        Returns the date typle (YYYY,MM,DD) give the name/title of the product
        @param title - title of the product
        @return: (YYYY,MM,DD) tuple
        '''
        match = dtreg.search(title)
        if match:
            return (match.group(1),match.group(2),match.group(3))
        else:
            raise RuntimeError("Failed to extract date from %s." % title)
    @staticmethod
    def getFileType():
        '''
        What filetype does this download
        '''
        return "zip"
    @classmethod
    def getSupportedType(clazz):
        '''
        Returns the name of the supported type for queries
        @return: type supported by this class
        '''
        return "scihub"

    #Non-required helpers
    def listAll(self,session,query,bbox,dns_alias):
        '''
        Lists the server for all products matching a query. 
        NOTE: this function also updates the global JSON querytime persistence file
        @param session - session to use for listing
        @param query - query to use to list products
        @param bbox - bounding box from AOI
        @param dns_alias - dns_alias to use if any
        @return list of (title,link) tuples
        '''
        dns = DEFAULT_DNS if dns_alias is None else dns_alias
        url_search = urljoin(dns, SEARCH_PATH)
        found=[]
        offset = 0
        loop = True
        while loop:
            query_params = { "q": query, "rows": 100, "format": "json", "start": offset }
            logger.info("query: %s" % json.dumps(query_params, indent=2))
            try:
                response = session.get(url_search, params=query_params, verify=False)
                logger.info("url: %s" % response.url)
                if response.status_code != 200:
                    logger.error("Error: %s\n%s" % (response.status_code,response.text))
                    raise qquery.query.QueryBadResponseException("Bad status")
                logger.info("response text: %s" % response.text)
                results = response.json()
            except:
                response = session.get(BACKUP_URL, params=query_params, verify=False, auth=('guest', 'guest'))
                logger.info("querying backup_url: %s" % response.url)
                if response.status_code != 200:
                    logger.error("Error: %s\n%s" % (response.status_code,response.text))
                    raise qquery.query.QueryBadResponseException("Bad status")
                logger.info("response text: %s" % response.text)
                results = response.json()
            with open('res.json', 'w') as f:
                f.write(json.dumps(results, indent=2))
            entries = results['feed'].get('entry', None)
            if entries is None: break
            with open('res.json', 'w') as f:
                f.write(json.dumps(entries, indent=2))
            if isinstance(entries, dict): entries = [ entries ] # if one entry, scihub doesn't return a list
            count = len(entries)
            offset += count
            loop = True if count > 0 else False
            logger.info("Found: {0} results".format(count))
            for item in entries:
                print(item)
                download_url = urljoin(dns, DOWNLOAD_PATH)
                dl_url = download_url.format(item['id'])
                name = item['title']
                found.append((name,dl_url))
        return found
