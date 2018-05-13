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
from subprocess import CalledProcessError

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    delivered_software,
    #support_software,
    runscript,
    prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs2nc as hirs2nc
import flo.sw.hirs_avhrr as hirs_avhrr
from flo.sw.hirs2nc.delta import DeltaCatalog
from flo.sw.hirs2nc.utils import link_files

# every module should have a LOG object
LOG = logging.getLogger(__name__)

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_CSRB_DAILY(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id', 'hirs_avhrr_delivery_id', 'hirs_csrb_daily_delivery_id']
    outputs = ['stats', 'means']

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id, hirs_csrb_daily_delivery_id):

        granules = [g.left for g in time_interval.overlapping_interval_series(timedelta(days=1),
                                                                              timedelta(days=1))]

        return [{'granule': g,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id,
                 'hirs_avhrr_delivery_id': hirs_avhrr_delivery_id,
                 'hirs_csrb_daily_delivery_id': hirs_csrb_daily_delivery_id}
                for g in granules]

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='CSRB')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        LOG.debug("Running build_task()")
        LOG.debug("context:  {}".format(context))

        # Initialize the hirs2nc and hirs_avhrr modules with the data locations
        hirs2nc.delta_catalog = delta_catalog
        hirs_avhrr.delta_catalog = delta_catalog

        # Instantiate the hirs and hirs_avhrr computations
        hirs2nc_comp = hirs2nc.HIRS2NC()
        hirs_avhrr_comp = hirs_avhrr.HIRS_AVHRR()

        SPC = StoredProductCatalog()

        day = TimeInterval(context['granule'], (context['granule'] + timedelta(days=1) -
                                                timedelta(seconds=1)))

        hirs2nc_contexts = hirs2nc_comp.find_contexts(day, context['satellite'], context['hirs2nc_delivery_id'])

        if len(hirs2nc_contexts) == 0:
            raise WorkflowNotReady('NO HIRS Data For {}'.format(context['granule']))

        # Input Counter.
        ic = 0

        for hirs2nc_context in hirs2nc_contexts:

            # Making Input contexts
            hirs_avhrr_context = hirs2nc_context.copy()
            hirs_avhrr_context['hirs_avhrr_delivery_id'] = context['hirs_avhrr_delivery_id']

            LOG.debug("HIRS context:        {}".format(hirs2nc_context))
            LOG.debug("HIRS_AVHRR context:  {}".format(hirs_avhrr_context))

            # Confirming we have HIRS1B and COLLO products...
            hirs2nc_prod = hirs2nc_comp.dataset('out').product(hirs2nc_context)
            hirs_avhrr_prod = hirs_avhrr_comp.dataset('out').product(hirs_avhrr_context)

            # If HIRS1B and COLLO products exist, add them and the Patmos-X
            # file for this context to the list of input files to be downloaded to
            # the workspace...
            if SPC.exists(hirs2nc_prod) and SPC.exists(hirs_avhrr_prod):
                # Its safe to require all three inputs
                task.input('HIR1B-{}'.format(ic), hirs2nc_prod)
                task.input('COLLO-{}'.format(ic), hirs_avhrr_prod)
                task.input('PTMSX-{}'.format(ic),
                           delta_catalog.file('avhrr', hirs2nc_context['satellite'],
                                              'PTMSX', hirs2nc_context['granule']))
                ic += 1


        LOG.debug("There are {} valid HIR1B/COLLO/PTMSX contexts in ({} -> {})".
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
            LOG.debug("Trying to retrieve CFSR_PGRBHANL product (pgbhnl.gdas.*.grb2) CFSR files from DAWG...")
            try:
                cfsr_files = dawg_catalog.files('', 'CFSR_PGRBHANL', interval)
                num_cfsr_files = len(cfsr_files)
                if num_cfsr_files == 0:
                    LOG.debug("\tpgbhnl.gdas.*.grb2 CFSR files from DAWG : {}".format(cfsr_files))
            except Exception, err :
                LOG.error("{}.".format(err))
                LOG.warn("Retrieval of CFSR_PGRBHANL product (pgbhnl.gdas.*.grb2) CFSR files from DAWG failed")

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2 files from PEATE
        if num_cfsr_files == 0:
            LOG.debug("Trying to retrieve CFSV2_PGRBHANL product (cdas1.*.t*z.pgrbhanl.grib2) CFSR files from DAWG...")
            try:
                cfsr_files = dawg_catalog.files('', 'CFSV2_PGRBHANL', interval)
                num_cfsr_files = len(cfsr_files)
                if num_cfsr_files == 0:
                    LOG.debug("\tcdas1.*.t*z.pgrbhanl.grib2 CFSR files from DAWG : {}".format(cfsr_files))
            except Exception, err :
                LOG.error("{}.".format(err))
                LOG.warn("Retrieval of CFSV2_PGRBHANL product cdas1.*.t*z.pgrbhanl.grib2 CFSR files from DAWG failed")

        LOG.debug("We've found {} CFSR files for context {}".format(len(cfsr_files),context))

        # Add the CFSR files to the list of input files to be downloaded to the
        # workspace...
        if num_cfsr_files != 0:
            for (i, cfsr_file) in enumerate(cfsr_files):
                task.input('CFSR-{}'.format(i), cfsr_file)
                LOG.debug("cfsr_file ({}) = {}".format(i, cfsr_file))

        LOG.debug("Final task.inputs...")
        for task_key in task.inputs.keys():
            LOG.debug("\t{}: {}".format(task_key,task.inputs[task_key]))

    def hirs_to_time_interval(self, filename):
        '''
        Takes the HIRS filename as input and returns the 1-day time interval
        covering that file.
        '''

        file_chunks = filename.split('.')
        begin_time = datetime.strptime('.'.join(file_chunks[3:5]), 'D%y%j.S%H%M')
        end_time = datetime.strptime('.'.join([file_chunks[3], file_chunks[5]]), 'D%y%j.E%H%M')

        if end_time < begin_time:
            end_time += timedelta(days=1)

        return TimeInterval(begin_time, end_time)

    #def time_interval_to_hirs(self, interval):

        #return '{}.{}'.format(interval.left.strftime('D%y%j.S%H%M'),
                              #interval.right.strftime('E%H%M'))

    #def context_path(self, context, output):

        #return pjoin('HIRS',
                            #'{}/{}'.format(context['satellite'], context['granule'].year),
                            #'CSRB_DAILY')

    def extract_bin_from_cfsr(self, inputs, context):
        '''
        Run wgrib2 on the  input CFSR grib files, to create flat binary files
        containing the desired data.
        NEW METHOD
        '''

        # Where are we running the package
        work_dir = abspath(curdir)
        LOG.debug("working dir = {}".format(work_dir))

        # Get the required CFSR and wgrib2 script locations
        hirs_csrb_daily_delivery_id = context['hirs_csrb_daily_delivery_id']
        delivery = delivered_software.lookup('hirs_csrb_daily', delivery_id=hirs_csrb_daily_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        extract_cfsr_bin = pjoin(dist_root, 'bin/extract_cfsr.csh')
        version = delivery.version

        # Get a list of CFSR inputs
        input_keys = inputs.keys()
        input_keys.sort()
        cfsr_keys = [key for key in input_keys if 'CFSR' in key]
        cfsr_files = [inputs[key] for key in cfsr_keys]

        LOG.debug("CFSR files: {}".format(cfsr_files))

        # Extract the desired datasets for each CFSR file
        rc = 0
        new_cfsr_files = []

        for cfsr_file in cfsr_files:
            output_cfsr_file = '{}.bin'.format(basename(cfsr_file))
            cmd = '{} {} {} {}'.format(extract_cfsr_bin, cfsr_file, output_cfsr_file, dirname(extract_cfsr_bin))
            #cmd = 'sleep 0; touch {}'.format(output_cfsr_file) # DEBUG

            try:
                LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
                rc_extract_cfsr = 0
                runscript(cmd, [delivery])
                new_cfsr_files.append('{}'.format(output_cfsr_file))
            except CalledProcessError as err:
                rc_extract_cfsr = err.returncode
                LOG.error("extract_cfsr binary {} returned a value of {}".format(extract_cfsr_bin, rc_extract_cfsr))
                return rc_extract_cfsr, []

            # Verify output file
            output_cfsr_file = glob(output_cfsr_file)
            if len(output_cfsr_file) != 0:
                output_cfsr_file = output_cfsr_file[0]
                LOG.info('Found flat CFSR file "{}"'.format(output_cfsr_file))
            else:
                LOG.error('Failed to genereate "{}", aborting'.format(output_cfsr_file))
                rc = 1
                return rc, []

        return rc, new_cfsr_files

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

    def get_orbital_intervals(self, inputs, context):
        '''
        Construct dictionaries of the orbital input data, and the associated time intervals.
        '''

        rc = 0

        # Get a list of HIR1B inputs
        input_keys = inputs.keys()
        input_keys.sort()
        HIR1B_keys = [key for key in input_keys if 'HIR1B' in key]
        #HIR1B = {}
        #file_glob = '{}/NSS.HIRX*.nc'.format(input_dir)
        #HIR1B = {'HIR1B_{}'.format(i):abspath(x) for (i,x) in enumerate(sorted(glob(file_glob)))}

        #PTMSX = {}
        #file_glob = '{}/NSS.GHRR*.hdf'.format(input_dir)
        #PTMSX = {'PTMSX_{}'.format(i):abspath(x) for (i,x) in enumerate(sorted(glob(file_glob)))}

        #COLLO = {}
        #file_glob = '{}/colloc.hirs.avhrr*.hdf'.format(input_dir)
        #COLLO = {'COLLO_{}'.format(i):abspath(x) for (i,x) in enumerate(sorted(glob(file_glob)))}

        file_indicies = xrange(len(HIR1B_keys))

        intervals = {}

        for idx in file_indicies:
            filename = basename(inputs['HIR1B-{}'.format(idx)])
            interval = self.hirs_to_time_interval(filename)
            LOG.debug("HIRS interval {}: {} -> {}, {}".format(idx, interval.left,interval.right, inputs['HIR1B-{}'.format(idx)]))
            intervals['interval-{}'.format(idx)] = interval

        return rc, intervals

    def create_cfsr_statistics(self, inputs, context, cfsr_bin_files):
        '''
        Create the CFSR statistics for the current day.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required CFSR and wgrib2 script locations
        hirs_csrb_daily_delivery_id = context['hirs_csrb_daily_delivery_id']
        delivery = delivered_software.lookup('hirs_csrb_daily', delivery_id=hirs_csrb_daily_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        lut_dir = pjoin(dist_root, 'luts')
        version = delivery.version

        # Compile a dictionary of the input orbital data files
        rc, intervals = self.get_orbital_intervals(inputs, context)

        # Determine the output filenames
        output_stats = 'csrb_daily_stats_{}_{}.nc'.format(context['satellite'],
                                                          context['granule'].strftime('D%y%j'))
        LOG.info("output_stats: {}".format(output_stats))

        # Link the coefficient files into the working directory
        shifted_coeffs = [abspath(x) for x in glob(pjoin(lut_dir,'shifted_hirs_FM_coeff/*'))]
        unshifted_coeffs = [abspath(x) for x in glob(pjoin(lut_dir,'unshifted_hirs_FM_coeff/*'))]
        linked_coeffs = link_files(current_dir, shifted_coeffs+
                                               unshifted_coeffs+
                                               [
                                                   abspath(pjoin(lut_dir, 'CFSR_lst.bin')),
                                                   abspath(pjoin(lut_dir, 'CO2_1979-2017_monthly_181_lat.dat'))
                                               ])

        LOG.debug("Linked coeffs: {}".format(linked_coeffs))

        csrb_daily_stats_bin = pjoin(dist_root, 'bin/process_csrb_cfsr.exe')
        debug = 0
        shifted_FM_opt = 2

        # Loop through the intervals
        indicies = xrange(len(intervals))

        for idx in indicies:

            interval = intervals['interval-{}'.format(idx)]
            LOG.debug("HIRS interval {}: {} -> {}".format(idx, interval.left,interval.right))

            cfsr_bin = self.cfsr_input(cfsr_bin_files, interval)
            LOG.debug("cfsr_bin ({}:{}): {}".format(idx, interval, cfsr_bin))

            if cfsr_bin == None:
                LOG.warn("Could not find cfsr_bin file to match HIRS interval: {} -> {}".format(interval.left,interval.right))
                continue

            cmd = '{} {} {} {} {} {} {} {} {} {}'.format(
                    csrb_daily_stats_bin,
                    inputs['HIR1B-{}'.format(idx)],
                    cfsr_bin,
                    inputs['COLLO-{}'.format(idx)],
                    inputs['PTMSX-{}'.format(idx)],
                    'CFSR_lst.bin',
                    'CO2_1979-2017_monthly_181_lat.dat',
                    debug,
                    shifted_FM_opt,
                    output_stats
                    )
            #cmd = 'sleep 1; touch {}'.format(output_stats) # DEBUG

            try:
                LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
                rc_csrb_cfsr = 0
                runscript(cmd, [delivery])
            except CalledProcessError as err:
                rc_csrb_cfsr = err.returncode
                LOG.error("csrb_cfsr binary {} returned a value of {}".format(csrb_daily_stats_bin, rc_csrb_cfsr))
                return rc_csrb_cfsr, None

        # Verify output file
        output_stats = glob(output_stats)
        if len(output_stats) != 0:
            output_stats = output_stats[0]
            LOG.info('Found output CFSR statistics file "{}"'.format(output_stats))
        else:
            LOG.error('Failed to genereate "{}", aborting'.format(output_stats))
            rc = 1
            return rc, None

        return rc, output_stats

    def create_cfsr_means(self, inputs, context, stats_file):
        '''
        Create the CFSR means for the current day.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required CFSR and wgrib2 script locations
        hirs_csrb_daily_delivery_id = context['hirs_csrb_daily_delivery_id']
        delivery = delivered_software.lookup('hirs_csrb_daily', delivery_id=hirs_csrb_daily_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        lut_dir = pjoin(dist_root, 'luts')
        version = delivery.version

        # Determine the output filenames
        output_means = basename(stats_file).replace('stats', 'means')
        LOG.info("output_means: {}".format(output_means))

        csrb_daily_means_bin = pjoin(dist_root, 'bin/create_daily_global_csrbs_netcdf.exe')
        shifted_FM_opt = 2

        cmd = '{} {} {} {}'.format(
                csrb_daily_means_bin,
                basename(stats_file),
                output_means,
                shifted_FM_opt
                )
        #cmd = 'sleep 1; touch {}'.format(output_means)

        LOG.info('\n'+cmd+'\n')

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_cfsr_means = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_cfsr_means = err.returncode
            LOG.error("csrb_cfsr binary {} returned a value of {}".format(csrb_daily_means_bin, rc_cfsr_means))
            return rc_cfsr_means, None

        # Verify output file
        output_means = glob(output_means)
        if len(output_means) != 0:
            output_means = output_means[0]
            LOG.info('Found output CFSR means file "{}"'.format(output_means))
        else:
            LOG.error('Failed to genereate "{}", aborting'.format(output_means))
            rc = 1
            return rc, None

        return rc, output_means

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='CSRB')
    def run_task(self, inputs, context):

        LOG.debug("Running run_task()...")

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        rc = 0

        # Extract a binary array from a CFSR reanalysis GRIB2 file on a
        # global equal angle grid at 0.5 degree resolution. CFSR files
        rc, cfsr_files = self.extract_bin_from_cfsr(inputs, context)

        # Create the CFSR statistics for the current day.
        rc, output_stats_file = self.create_cfsr_statistics(inputs, context, cfsr_files)
        if rc != 0:
            return rc
        LOG.debug('create_cfsr_statistics() generated {}...'.format(output_stats_file))

        # Create the CFSR means for the current day
        rc, output_means_file = self.create_cfsr_means(inputs, context, output_stats_file)
        if rc != 0:
            return rc
        LOG.debug('create_cfsr_means() generated {}...'.format(output_means_file))

        LOG.debug('python return value = {}'.format(rc))

        extra_attrs = {
                       'begin_time': context['granule'],
                       'end_time': context['granule']+timedelta(days=1)-timedelta(seconds=1)
                      }

        LOG.debug('extra_attrs = {}'.format(extra_attrs))

        return {'stats': {'file': nc_compress(output_stats_file), 'extra_attrs': extra_attrs},
                'means': {'file': nc_compress(output_means_file), 'extra_attrs': extra_attrs}}
