from datetime import datetime, timedelta
from flo.time import TimeInterval
from flo.sw.hirs_csrb_daily import HIRS_CSRB_DAILY
from flo.ui import submit_order
from flo.config import config
import logging
import sys
import psycopg2
import time


def submit(logger, interval, platform):

    hirs_version = 'v20140204'
    collo_version = 'v20140204'
    csrb_version = 'v20140204'

    c = HIRS_CSRB_DAILY()
    contexts = c.find_contexts(platform, hirs_version, collo_version, csrb_version, interval)

    while 1:
        try:
            return submit_order(c, [c.dataset('means')], contexts)
        except:
            time.sleep(5*60)
            logger.info('Failed submiting jobs for.  Sleeping for 5 minutes and submitting again')

# Setup Logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

# Submitting Jobs
for platform in ['metop-a']:
    for interval in [TimeInterval(datetime(2009, 1, 1), datetime(2009, 2, 1))]:
        jobIDRange = submit(logger, interval, platform)

        if len(jobIDRange) > 0:
            logger.info('Submitting hirs_csrb_daily jobs for {} from {} to {}'.format(platform,
                                                                                      interval.left,
                                                                                      interval.right))
        else:
            logger.info('No hirs_csrb_daily jobs for {} from {} to {}'.format(platform,
                                                                              interval.left,
                                                                              interval.right))


            
