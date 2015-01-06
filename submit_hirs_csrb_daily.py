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
    hirs_csrb_daily_version = 'v20140204'

    c = HIRS_CSRB_DAILY()
    contexts = c.find_contexts(platform, hirs_version, hirs_csrb_daily_version, interval)

    while 1:
        try:
            return submit_order(c, [c.dataset('means'), c.dataset('stats')], contexts)
        except:
            time.sleep(5*60)
            logger.info('Failed submiting jobs for.  Sleeping for 5 minutes and submitting again')

def fix_unreaped_jobs(jobIDRange):
    # this will re-enter jobs that got reaped before they made it into condor
    conn = psycopg2.connect(**config.get()['database'])
    cur = conn.cursor()
    cmd = "insert into unreaped_jobs select generate_series(%d,%d) \
	except all select job from unreaped_jobs" % (jobIDRange[0],jobIDRange[-1])
    cur.execute(cmd)
    conn.commit()
    conn.close()
    

# Setup Logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

# Submitting Jobs
for platform in ['metop-a']:
    for interval in [TimeInterval(datetime(2009, 1, 1), datetime(2009, 2, 1))]:
        jobIDRange = submit(logger, interval, platform)

        if len(jobIDRange) > 0:
            fix_unreaped_jobs(jobIDRange)
            logger.info('Submitting hirs_csrb_daily jobs for {} from {} to {}'.format(platform,
                                                                                      interval.left,
                                                                                      interval.right))
        else:
            logger.info('No hirs_csrb_daily jobs for {} from {} to {}'.format(platform,
                                                                              interval.left,
                                                                              interval.right))


            
