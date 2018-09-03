#!/usr/bin/env python
from __future__ import print_function

import json
import re
import argparse
import datetime
import os
import sys 
import requests
from requests.auth import HTTPBasicAuth
from shapely.geometry.polygon import Polygon
from urlparse import urljoin


import qquery.query
import xml.dom.minidom


'''
This worker queries slc products from scihub copernicus
'''

#Global constants
DEFAULT_DNS = "https://scihub.copernicus.eu/"
API_PATH = "apihub/odata/v1/"
dtreg = re.compile(r'S1\w_.+?_(\d{4})(\d{2})(\d{2})T.*')
QUERY_TEMPLATE = "IngestionDate ge datetime'{0}' and IngestionDate lt datetime'{1}' and substringof('SLC',Name)"

class SciHubODATA(qquery.query.AbstractQuery):
    '''
    Sci-hub query implementer
    '''
    def query(self,start,end,aoi,dns_alias):
        '''
        Performs the actual query, and returns a list of (title, url) tuples
        @param start - start time stamp
        @param end - end time stamp
        @param aoi - area of interest
        @return: list of (title, url) pairs
        '''
        session = requests.session()
        qur = QUERY_TEMPLATE.format(start,end)

        return self.listAll(session,qur,aoi["location"]["coordinates"], dns_alias)
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
        return ("0000","00","00")
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
    def listAll(self,session,query,bbox, dns_alias):
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
        url_api = urljoin(dns, API_PATH)

        found=[]
        offset = 0
        loop = True
        while loop:
            response = session.get(urljoin(url_api, "Products/?"), params={"$filter":query, "$skip":offset, "$top":100,"$format":"json"})
            if response.status_code != 200:
                print("Error: %s\n%s" % (response.status_code,response.text))
                raise qquery.query.QueryBadResponseException("Bad status")
            results = json.loads(response.text)["d"]["results"]
            count = len(results)
            offset += count
            loop = True if count > 0 else False
            print("Found: {0} results".format(count))
            for item in results:
                if self.intersects(item["ContentGeometry"],bbox):
                    # Hack to rewrite URL because API gives wrong URL
                    download_url=item["__metadata"]["media_src"]
                    download_url=download_url.replace("https://scihub.copernicus.eu/odata/v1", url_api)
                    found.append((item["Name"],download_url))

                    # found.append((item["Name"],item["__metadata"]["media_src"]))
        return found
    def intersects(self,gml,bbox):
        '''
        Does the GML intersect the bounding box?
        @param gml - Geo XML blob
        @param bbox - bbox from AOI
        @returns True/False
        '''
        ring=[]
        points = xml.dom.minidom.parseString(gml).getElementsByTagName("gml:coordinates")[0].firstChild.nodeValue
        for point in points.split(" "):
            splits = point.split(",")
            lat = float(splits[0])
            lon = float(splits[1])
            ring.append([lon,lat])
        for box in bbox:
            polybbox = Polygon(box)
            polyring = Polygon(ring)
            if polybbox.intersects(polyring):
                return True
        return False
