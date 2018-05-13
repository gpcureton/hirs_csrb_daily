#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_csrb_daily package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import sys
import traceback
import logging

from timeutil import TimeInterval, datetime, timedelta
from flo.ui import local_prepare, local_execute

import flo.sw.hirs2nc as hirs2nc
import flo.sw.hirs_avhrr as hirs_avhrr
import flo.sw.hirs_csrb_daily as hirs_csrb_daily

from flo.sw.hirs2nc.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

#
# General information
#

#hirs2nc_delivery_id = '20180410-1'
#hirs_avhrr_delivery_id = '20180505-1'
#hirs_csrb_daily_delivery_id  = '20180511-1'
wedge = timedelta(seconds=1.)

# Satellite specific information

#granule = datetime(2017, 1, 1, 0, 32)
#interval = TimeInterval(granule, granule+timedelta(seconds=0))

def setup_computation(satellite):

    input_data = {'HIR1B': '/mnt/software/flo/hirs_l1b_datalists/{0:}/HIR1B_{0:}_latest'.format(satellite),
                  'CFSR':  '/mnt/cephfs_data/geoffc/hirs_data_lists/CFSR.out',
                  'PTMSX': '/mnt/software/flo/hirs_l1b_datalists/{0:}/PTMSX_{0:}_latest'.format(satellite)}

    # Data locations
    collection = {'HIR1B': 'ILIAD',
                  'CFSR': 'DELTA',
                  'PTMSX': 'FJORD'}

    input_sources = {'collection':collection, 'input_data':input_data}

    # Initialize the hirs2nc module with the data locations
    hirs_csrb_daily.set_input_sources(input_sources)

    # Instantiate the computation
    comp = hirs_csrb_daily.HIRS_CSRB_DAILY()

    return comp


#
# Local execution
#

def local_execute_example(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id, hirs_csrb_daily_delivery_id,
                          skip_prepare=False, skip_execute=False, verbosity=2):

    setup_logging(verbosity)

    comp = setup_computation(satellite)
    hirs2nc_comp = hirs2nc.HIRS2NC()
    hirs_avhrr_comp = hirs_avhrr.HIRS_AVHRR()

    # Get the required context...
    contexts =  comp.find_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id, hirs_csrb_daily_delivery_id)

    if len(contexts) != 0:
        LOG.info("Candidate contexts in interval...")
        for context in contexts:
            print("\t{}".format(context))

        try:
            if not skip_prepare:
                LOG.info("Running hirs_csrb_daily local_prepare()...")
                LOG.info("Preparing context... {}".format(contexts[0]))
                local_prepare(comp, contexts[0], download_only=[hirs2nc_comp, hirs_avhrr_comp])
            if not skip_execute:
                LOG.info("Running hirs_csrb_daily local_execute()...")
                LOG.info("Running context... {}".format(contexts[0]))
                local_execute(comp, contexts[0])
        except Exception, err:
            LOG.error("{}".format(err))
            LOG.debug(traceback.format_exc())
    else:
        LOG.error("There are no valid {} contexts for the interval {}.".format(satellite, interval))


def print_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id, hirs_csrb_daily_delivery_id, verbosity=2):

    setup_logging(verbosity)

    comp = setup_computation(satellite)

    contexts = comp.find_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id, hirs_csrb_daily_delivery_id)
    for context in contexts:
        LOG.info(context)

#satellite_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    #'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    #'noaa-19', 'metop-a', 'metop-b']
