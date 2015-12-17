import os,sys
import time
from datetime import datetime
from flo.time import TimeInterval
from flo.ui import local_prepare, local_execute

from flo.sw.hirs_csrb_daily import HIRS_CSRB_DAILY

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)


# Set up the logging
console_logFormat = '%(asctime)s : (%(levelname)s):%(filename)s:%(funcName)s:%(lineno)d:  %(message)s'
dateFormat = '%Y-%m-%d %H:%M:%S'
levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level = levels[2], 
        format = console_logFormat, 
        datefmt = dateFormat)


def local_execute_example(sat, hirs_version, collo_version, csrb_version, granule):
    try:
        LOG.info("Running local_prepare()") # GPC
        local_prepare(HIRS_CSRB_DAILY(), { 
                                      'sat': sat, 
                                      'hirs_version': hirs_version, 
                                      'collo_version': collo_version, 
                                      'csrb_version': csrb_version,
                                      'granule': granule
                                      }
                                      )
        LOG.info("Running local_execute()") # GPC
        local_execute(HIRS_CSRB_DAILY(), {
                                      'sat': sat, 
                                      'hirs_version': hirs_version, 
                                      'collo_version': collo_version, 
                                      'csrb_version': csrb_version,
                                      'granule': granule
                                      }
                                      )
    except Exception, err :
        LOG.error("{}.".format(err))
        LOG.debug(traceback.format_exc())



sat = 'metop-b'
hirs_version  = 'v20151014'
collo_version = 'v20151014'
csrb_version  = 'v20150915'
granule = datetime(2014, 1, 15, 0, 0)

#local_execute_example(sat, hirs_version, collo_version, csrb_version, granule)
