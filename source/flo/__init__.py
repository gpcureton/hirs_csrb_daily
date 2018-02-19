#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_csrb_daily package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
from glob import glob
import shutil
import logging
import traceback

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
#from flo.subprocess import check_call
from flo.product import StoredProductCatalog
from flo.ingest import IngestCatalog

import sipsprod
from glutil import (
    check_call,
    #dawg_catalog,
    #delivered_software,
    #support_software,
    #runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs as hirs
import flo.sw.hirs_avhrr as hirs_avhrr
from flo.sw.hirs.delta import DeltaCatalog

# every module should have a LOG object
LOG = logging.getLogger(__name__)

ingest_catalog = IngestCatalog('PEATE')

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_CSRB_DAILY(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version']
    outputs = ['stats', 'means']

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='CSRB')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        LOG.debug("Running build_task()")
        LOG.debug("context:  {}".format(context))
        LOG.debug("Initial task.inputs:  {}".format(task.inputs))

        # Initialize the hirs and hirs_avhrr modules with the data locations
        hirs.delta_catalog = delta_catalog
        hirs_avhrr.delta_catalog = delta_catalog
        # Instantiate the hirs and hirs_avhrr computations
        hirs_comp = hirs.HIRS()
        hirs_avhrr_comp = hirs_avhrr.HIRS_AVHRR()

        SPC = StoredProductCatalog()

        day = TimeInterval(context['granule'], (context['granule'] + timedelta(days=1) -
                                                timedelta(seconds=1)))

        hirs_contexts = hirs_comp.find_contexts(day, context['sat'], context['hirs_version'])

        if len(hirs_contexts) == 0:
            raise WorkflowNotReady('NO HIRS Data For {}'.format(context['granule']))

        # Input Counter.
        ic = 0

        #for hirs_context in hirs_contexts:
        for hirs_context in hirs_contexts[:1]:

            # Making Input contexts
            hirs_avhrr_context = hirs_context.copy()
            hirs_avhrr_context['collo_version'] = context['collo_version']

            LOG.debug("HIRS context:  {}".format(hirs_context))
            LOG.debug("HIRS_AVHRR context:  {}".format(hirs_avhrr_context))

            # Confirming we have HIRS1B and COLLO products...
            hirs_prod = hirs_comp.dataset('out').product(hirs_context)
            hirs_avhrr_prod = hirs_avhrr_comp.dataset('out').product(hirs_avhrr_context)

            # If HIRS1B and COLLO products exist, add them and the Patmos-X
            # file for this context to the list of input files to be downloaded to 
            # the workspace...
            if SPC.exists(hirs_prod) and SPC.exists(hirs_avhrr_prod):
                # Its safe to require all three inputs
                task.input('HIR1B-{}'.format(ic), hirs_prod)
                task.input('COLLO-{}'.format(ic), hirs_avhrr_prod)
                task.input('PTMSX-{}'.format(ic),
                           delta_catalog.file('avhrr', hirs_context['sat'],
                                              'PTMSX', hirs_context['granule']))
                ic += 1


        LOG.info("There are {} valid HIR1B/COLLO/PTMSX contexts in ({} -> {})".
                format(ic,day.left,day.right))


        if ic == 0:
            LOG.warn("There are no valid HIR1B/COLLO/PTMSX contexts in ({} -> {}), aborting...".
                    format(day.left,day.right))
            return

        interval = TimeInterval(context['granule'], 
                                context['granule'] + timedelta(days=1))

        num_cfsr_files = 0

        # Search for the old style pgbhnl.gdas.*.grb2 files from the PEATE
        if num_cfsr_files == 0:
            LOG.debug("Trying to retrieve pgbhnl.gdas.*.grb2 CFSR files from PEATE...")
            try:
                cfsr_files = ingest_catalog.files('CFSR_PGRBHANL',interval)
                num_cfsr_files = len(cfsr_files)
                if num_cfsr_files == 0:
                    LOG.debug("\tpgbhnl.gdas.*.grb2 CFSR files from PEATE : {}".format(cfsr_files))
            except Exception, err :
                LOG.error("{}.".format(err))
                LOG.warn("Retrieval of pgbhnl.gdas.*.grb2 CFSR files from PEATE failed")

        # Search for the old style pgbhnl.gdas.*.grb2 files from the file list
        #if num_cfsr_files == 0:
            #LOG.debug("Trying to retrieve pgbhnl.gdas.*.grb2 CFSR files from DELTA...")
            #try:
                #cfsr_files = delta_catalog.files('ancillary', 'NONE', 'CFSR', interval)
                #num_cfsr_files = len(cfsr_files)
                #LOG.debug("pgbhnl.gdas.*.grb2 CFSR files from DELTA : {}".format(cfsr_files))
            #except Exception, err :
                #LOG.error("{}.".format(err))
                #LOG.warn("Retrieval of pgbhnl.gdas.*.grb2 CFSR files from DELTA failed")

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2 files from PEATE
        if num_cfsr_files == 0:
            LOG.debug("Trying to retrieve cdas1.*.t*z.pgrbhanl.grib2 CFSR files from PEATE...")
            try:
                cfsr_files = ingest_catalog.files('CFSV2_PGRBHANL',interval)
                num_cfsr_files = len(cfsr_files)
                if num_cfsr_files == 0:
                    LOG.debug("\tcdas1.*.t*z.pgrbhanl.grib2 CFSR files from PEATE : {}".format(cfsr_files))
            except Exception, err :
                LOG.error("{}.".format(err))
                LOG.warn("Retrieval of cdas1.*.t*z.pgrbhanl.grib2 CFSR files from PEATE failed")

        LOG.info("We've found {} CFSR files for context {}".format(len(cfsr_files),context))

        # Add the CFSR files to the list of input files to be downloaded to the 
        # workspace...
        if num_cfsr_files != 0:
            for (i, cfsr_file) in enumerate(cfsr_files):
                task.input('CFSR-{}'.format(i), cfsr_file)
                LOG.debug("cfsr_file ({}) = {}".format(i, cfsr_file))

        LOG.debug("Final task.inputs...")
        for task_key in task.inputs.keys():
            LOG.debug("\t{}: {}".format(task_key,task.inputs[task_key]))


    def generate_cfsr_bin(self, context):

        shutil.copy(pjoin(self.package_root, context['csrb_version'],
                                 'bin/wgrib2'), './')

        # Search for the old style pgbhnl.gdas.*.grb2 files
        files = glob('pgbhnl.gdas.*.grb2')

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2
        if len(files)==0:
            files = glob('cdas1.*.pgrbhanl.grib2')

        LOG.debug("CFSR files: {}".format(files))

        new_cfsr_files = []
        for file in files:
            cmd = pjoin(self.package_root, context['csrb_version'],
                               'bin/extract_cfsr.csh')
            cmd += ' {} {}.bin ./'.format(file, file)

            print cmd
            try:
                check_call(cmd, shell=True)
                new_cfsr_files.append('{}.bin'.format(file))
            except:
                pass

        return new_cfsr_files


    def cfsr_input(self, cfsr_bin_files, interval):

        # Get the CFSR datetime (00z, 06z, 12z, 18z, 00z) which is closest to the start
        # of the HIRS interval
        cfsr_granule = round_datetime(interval.left, timedelta(hours=6))

        # Construct old and new CFSR filenames based on the CFSR datetime
        pgbhnl_filename = 'pgbhnl.gdas.{}.grb2.bin'.format(cfsr_granule.strftime('%Y%m%d%H'))
        cdas1_filename = 'cdas1.{}.t{}z.pgrbhanl.grib2.bin'.format(cfsr_granule.strftime('%Y%m%d'),
                cfsr_granule.strftime('%H'))
        LOG.debug("pgbhnl_filename file is {}".format(pgbhnl_filename))
        LOG.debug("cdas1_filename file is {}".format(cdas1_filename))
        
        for files in cfsr_bin_files:
            LOG.debug("Candidate file is {}".format(files))
            if files == pgbhnl_filename:
                LOG.debug("We have a CFSR file match: {}".format(files))
                return files
            elif files == cdas1_filename:
                LOG.debug("We have a CFSR file match: {}".format(files))
                return files
            else:
                pass

        return None


    @reraise_as(WorkflowNotReady, FileNotFound, prefix='CSRB')
    def run_task(self, inputs, context):

        LOG.info("Running run_task()")
        LOG.info("context:  {}".format(context))

        if inputs == {}:
            LOG.warn("There are no valid inputs for context {}, aborting...".
                    format(context['granule']))
            return {}

        input_keys = inputs.keys()
        input_keys.sort()
        for key in input_keys:
            LOG.debug("{:8s} : {}".format(key,inputs[key]))

        debug = 0
        shifted_FM_opt = 2

        #print "The inputs to symlink are {}".format(inputs)
        inputs = symlink_inputs_to_working_dir(inputs)

        # Counting number of HIR1B inputs
        num_inputs = len([input for input in inputs.keys() if 'HIR1B' in input])

        # Output names
        output_stats = 'csrb_daily_stats_{}_{}.nc'.format(context['sat'],
                                                          context['granule'].strftime('D%y%j'))
        output_means = 'csrb_daily_means_{}_{}.nc'.format(context['sat'],
                                                          context['granule'].strftime('D%y%j'))

        # Netcdf Fortran Libraries
        lib_dir = pjoin(self.package_root, context['csrb_version'], 'lib')

        # Copy coeffs to working directory
        for f in glob(pjoin(self.package_root,
                                   context['csrb_version'], 'coeffs/*')):
            shutil.copy(f, './')

        # Converting CFSR grib inputs to binary
        cfsr_bin_files = self.generate_cfsr_bin(context)


        # Running csrb daily stats for each HIR1B input
        for i in xrange(num_inputs):
            
            interval = self.hirs_to_time_interval(inputs['HIR1B-{}'.format(i)])
            LOG.debug("HIRS interval: {} -> {}".format(interval.left,interval.right))

            cfsr_bin = self.cfsr_input(cfsr_bin_files,interval)
            LOG.debug("cfsr_bin ({}): {}".format(i,cfsr_bin))

            if cfsr_bin == None:
                LOG.warn("Could not find cfsr_bin file to match HIRS interval: {} -> {}".format(interval.left,interval.right))
                continue

            cmd = pjoin(self.package_root, context['csrb_version'],
                               'bin/process_csrb_cfsr.exe')
            cmd += ' ' + inputs['HIR1B-{}'.format(i)]
            cmd += ' ' + cfsr_bin
            cmd += ' ' + inputs['COLLO-{}'.format(i)]
            cmd += ' ' + inputs['PTMSX-{}'.format(i)]
            cmd += ' ' + pjoin(self.package_root,
                                      context['csrb_version'],
                                      'CFSR_lst.bin')
            cmd += ' {} {}'.format(debug, shifted_FM_opt)
            cmd += ' ' + output_stats

            print cmd


            # Sometimes this fails due to bad inputs.  Its better to have a 1/2 day of data than
            # no day of data.
            try:
                check_call(cmd, shell=True,
                           env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))
            except:
                LOG.warn('ORBIT FAILED: {}'.format(inputs['HIR1B-{}'.format(i)]))


        # Running csrb daily means
        cmd = pjoin(self.package_root, context['csrb_version'],
                           'bin/create_daily_global_csrbs_netcdf.exe')
        cmd += ' {} {} {}'.format(output_stats, output_means, shifted_FM_opt)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'stats': output_stats, 'means': output_means}


    def find_contexts(self, time_interval, sat, hirs_version, collo_version, csrb_version):

        granules = [g.left for g in time_interval.overlapping_interval_series(timedelta(days=1),
                                                                              timedelta(days=1))]

        return [{'granule': g,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version}
                for g in granules]


    def hirs_to_time_interval(self, filename):
        '''
        Takes the HIRS filename as input and returns the 1-day time interval
        covering that file.
        '''

        begin_time = datetime.strptime(filename[12:24], 'D%y%j.S%H%M')
        end_time = datetime.strptime(filename[12:19]+filename[25:30], 'D%y%j.E%H%M')
        if end_time < begin_time:
            end_time += timedelta(days=1)

        return TimeInterval(begin_time, end_time)

    def time_interval_to_hirs(self, interval):

        return '{}.{}'.format(interval.left.strftime('D%y%j.S%H%M'),
                              interval.right.strftime('E%H%M'))

    def context_path(self, context, output):

        return pjoin('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CSRB_DAILY')
