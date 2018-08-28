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

import qquery.query
from hysds.orchestrator import submit_job


'''
This worker queries slc products and (untested) other products from: https://scihub.esa.int/dhus and
submits them as a download job.
'''

#Global constants
url = "https://scihub.esa.int/dhus/search?"
backup_url = "https://tmphub.copernicus.eu/dhus/search?"
treg = re.compile("<title>([^<]+)</title>")
lreg = re.compile("<link href=\"([^\"]+)\"")
dtreg = re.compile(r'S1[AB]_.+?_(\d{4})(\d{2})(\d{2})T.*')

class SciHub(qquery.query.AbstractQuery):
    '''
    Sci-hub query implementer
    '''
    def query(start,end,aoi):
        '''
        Performs the actual query, and returns a list of (title, url) tuples
        @param start - start time stamp
        @param end - end time stamp
        @param aoi - area of interest
        @return: list of (title, url) pairs
        '''
        session = requests.session()
        qur = self.buildQuery(start,end,"slc",aoi["bounds"])
        return self.listAll(session,qur)
        
    def getDataDateFromTitle(title):
        '''
        Returns the date typle (YYYY,MM,DD) give the name/title of the product
        @param title - title of the product
        @return: (YYYY,MM,DD) tuple
        '''
        match = dtreg.search(title)
        if match:
            return (match.groups(1),match.groups(2),match.groups(3))
        return ("0000","00","00")
        pass
    
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
    def listAll(session,query):
        '''
        Lists the server for all products matching a query.
        NOTE: this function also updates the global JSON querytime persistence file
        @param session - session to use for listing
        @param query - query to use to list products
        @return list of (title,link) tuples
        '''
        print("Listing: "+query)
        try:
            response = session.get(url,params={"q":query,"rows":1000})
            title = None
            found = []
            print(response)
            if response.status_code != 200:
                print("Error: %s\n%s" % (response.status_code,response.text))
                raise QueryBadResponseException("Bad status")
        except:
            print('attempting query over backup url: %s' % backup_url)
            response = session.get(backup_url,params={"q":query,"rows":1000}, auth=('guest', 'guest'))
            title = None
            found = []
            print(response)
            if response.status_code != 200:
                print("Error: %s\n%s" % (response.status_code,response.text))
                raise QueryBadResponseException("Bad status")
        for line in response.text.split("\n"):
            tmat = treg.match(line)
            lmat = lreg.match(line)
            if title is None and tmat:
                title = tmat.group(1)
            elif not title is None and lmat:
                link = lmat.group(1)
                found.append((title,link))
            else:
                title = None
        return found

    def buildQuery(start,end,type,bounds):
        '''
        Builds a query for the system
        @param start - start time in 
        @param end - end time in "NOW" format
        @param type - type in "slc" format
        @param bounds - bounds for refion to query
        @return query for talking to the system
        '''
        ply = ",".join([" ".join([str(dig) for dig in point]) for point in bounds])
        p="POLYGON(("+ply+"))"
        q ="ingestionDate:["+start+" TO "+end+"] AND "
        q+="productType="+type+" AND "
        q+="footprint:\"Intersects("+p+")\""
        return q

