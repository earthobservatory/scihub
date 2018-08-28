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

import qquery.query
import xml.dom.minidom

allowable_products = ["S1_IW_SLC", "S1_GRD"] #searchable product types
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('scihub_stub_query')


'''
This worker queries slc products from scihub copernicus
'''

#Global constants
url = "https://scihub.copernicus.eu/dhus/api/stub/products?"
download_url = "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/$value"
dtreg = re.compile(r'S1\w_.+?_(\d{4})(\d{2})(\d{2})T.*')
S1_QUERY_TEMPLATE = '( footprint:"Intersects(POLYGON(({0})))" ) AND ( beginPosition:[{1} TO {2}] AND endPosition:[{1} TO {2}] ) AND ( platformname:Sentinel-1 AND producttype:SLC )'
GRD_QUERY_TEMPLATE = '( ( beginPosition:[{1} TO {2}] AND endPosition:[{1} TO {2}] ) AND ( producttype:GRD )'


class SciHubODATAStub(qquery.query.AbstractQuery):
    '''
    Sci-hub query implementer
    '''
    def query(self,start,end,aoi,mapping='S1_IW_SLC'):
        '''
        Performs the actual query, and returns a list of (title, url) tuples
        @param start - start time stamp
        @param end - end time stamp
        @param aoi - area of interest
        @mapping - type of product to queried. defaults to S1_IW_SLC
        @return: list of (title, url) pairs
        '''
        session = requests.session()
        if mapping == "S1_IW_SLC":
            polygon = ",".join(["%s %s" % (i[0], i[1]) for i in aoi["location"]["coordinates"][0]])
            qur = S1_QUERY_TEMPLATE.format(polygon,"%sZ" % start,"%sZ" % end)
            return self.listAll(session,qur,aoi["location"]["coordinates"])
        elif mapping == "GRD":
            qur = GRD_QUERY_TEMPLATE.format(polygon,"%sZ" % start,"%sZ" % end)
            return self.listAll(session,qur)


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
    def listAll(self,session,query,bbox):
        '''
        Lists the server for all products matching a query. 
        NOTE: this function also updates the global JSON querytime persistence file
        @param session - session to use for listing
        @param query - query to use to list products
        @param bbox - bounding box from AOI
        @return list of (title,link) tuples
        '''
        found=[]
        offset = 0
        loop = True
        while loop:
            query_params = { "filter": query, "offset": offset, "limit": 100 }
            logger.info("query: %s" % json.dumps(query_params, indent=2))
            query_url = url + "&".join(["%s=%s" % (i, query_params[i]) for i in query_params]).replace("(", "%28").replace(")", "%29")
            response = session.get(query_url)
            logger.info("url: %s" % response.url)
            if response.status_code != 200:
                logger.error("Error: %s\n%s" % (response.status_code,response.text))
                raise qquery.query.QueryBadResponseException("Bad status")
            logger.info("response text: %s" % response.text)
            results = json.loads(response.text)
            with open('res.json', 'w') as f:
                f.write(json.dumps(results, indent=2))
            count = len(results)
            offset += count
            loop = True if count > 0 else False
            logger.info("Found: {0} results".format(count))
            for item in results:
                dl_url = download_url.format(item['uuid'])
                name = item['identifier']
                found.append((name,dl_url))
        return found
