__version__     = "0.0.1"
__url__         = "http://gitlab.jpl.nasa.gov:8000/browser/trunk/HySDS/hysds"
__description__ = "SciHub"

def getHandler():
    '''
    Get the handler from this package
    '''
    #Import inside the funtion, to prevent problems loading module
    import scihub.scihub_opensearch_query
    return scihub.scihub_opensearch_query.SciHubOpenSearch()
