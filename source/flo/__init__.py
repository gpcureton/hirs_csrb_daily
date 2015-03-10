
from datetime import datetime, timedelta
from glob import glob
import os
import shutil
from flo.builder import WorkflowNotReady
from flo.computation import Computation
from flo.subprocess import check_call
from flo.time import round_datetime, TimeInterval
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.sw.hirs import HIRS
from flo.sw.hirs_avhrr import HIRS_AVHRR
from flo.sw.hirs.delta import delta_catalog


class HIRS_CSRB_DAILY(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version']
    outputs = ['stats', 'means']

    def build_task(self, context, task):

        day = TimeInterval(context['granule'], (context['granule'] + timedelta(days=1) -
                                                timedelta(seconds=1)))

        hirs_contexts = HIRS().find_contexts(context['sat'], context['hirs_version'], day)

        if len(hirs_contexts) == 0:
            raise WorkflowNotReady('NO HIRS Data For {}'.format(context['granule']))

        for (i, c) in enumerate(hirs_contexts):
            task.input('HIR1B-{}'.format(i), HIRS().dataset('out').product(c))
            # Adding collo_version parm to hirs_avhrr context
            c['collo_version'] = context['collo_version']
            task.input('COLLO-{}'.format(i), HIRS_AVHRR().dataset('out').product(c))
            task.input('PTMSX-{}'.format(i), delta_catalog.file('avhrr', c['sat'],
                                                                'PTMSX', c['granule']))

        cfsr_files = delta_catalog.files('ancillary', 'NONE', 'CFSR',
                                         TimeInterval(context['granule'],
                                                      (context['granule'] +
                                                       timedelta(days=1))))

        for (i, cfsr_file) in enumerate(cfsr_files):
            task.input('CFSR-{}'.format(i), cfsr_file)

    def run_task(self, inputs, context):

        debug = 0
        shifted_FM_opt = 2

        inputs = symlink_inputs_to_working_dir(inputs)
        # Counting number of HIR1B inputs
        num_inputs = len([input for input in inputs.keys() if 'HIR1B' in input])

        # Output names
        output_stats = 'csrb_daily_stats_{}_{}.nc'.format(context['sat'],
                                                          context['granule'].strftime('D%y%j'))
        output_means = 'csrb_daily_means_{}_{}.nc'.format(context['sat'],
                                                          context['granule'].strftime('D%y%j'))

        # Netcdf Fortran Libraries
        lib_dir = os.path.join(self.package_root, context['csrb_version'], 'lib')

        # Copy coeffs to working directory
        for f in glob(os.path.join(self.package_root,
                                   context['csrb_version'], 'coeffs/*')):
            shutil.copy(f, './')

        # Converting CFSR inputs to binary
        self.generate_cfsr_bin(context)

        # Running csrb daily stats for each HIR1B input
        for i in xrange(num_inputs):
            interval = self.hirs_to_time_interval(inputs['HIR1B-{}'.format(i)])
            cfsr_bin = self.cfsr_input(interval)

            cmd = os.path.join(self.package_root, context['csrb_version'],
                               'bin/process_csrb_cfsr.exe')
            cmd += ' ' + inputs['HIR1B-{}'.format(i)]
            cmd += ' ' + cfsr_bin
            cmd += ' ' + inputs['COLLO-{}'.format(i)]
            cmd += ' ' + inputs['PTMSX-{}'.format(i)]
            cmd += ' ' + os.path.join(self.package_root,
                                      context['csrb_version'],
                                      'CFSR_lst.bin')
            cmd += ' {} {}'.format(debug, shifted_FM_opt)
            cmd += ' ' + output_stats

            print cmd
            check_call(cmd, shell=True,
                       env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        # Running csrb daily means
        cmd = os.path.join(self.package_root, context['csrb_version'],
                           'bin/create_daily_global_csrbs_netcdf.exe')
        cmd += ' {} {} {}'.format(output_stats, output_means, shifted_FM_opt)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'stats': output_stats, 'means': output_means}

    def cfsr_input(self, interval):

        cfsr_granule = round_datetime(interval.left, timedelta(hours=6))
        return 'pgbhnl.gdas.{}.grb2.bin'.format(cfsr_granule.strftime('%Y%m%d%H'))

    def generate_cfsr_bin(self, context):

        shutil.copy(os.path.join(self.package_root, context['csrb_version'],
                                 'bin/wgrib2'), './')

        files = glob('pgbhnl.gdas.*.grb2')

        for file in files:
            cmd = os.path.join(self.package_root, context['csrb_version'],
                               'bin/extract_cfsr.csh')
            cmd += ' {} {}.bin ./'.format(file, file)

            print cmd
            check_call(cmd, shell=True)

    def find_contexts(self, sat, hirs_version, collo_version, csrb_version, time_interval):

        granules = [g.left for g in time_interval.overlapping_interval_series(timedelta(days=1),
                                                                              timedelta(days=1))]

        return [{'granule': g, 'sat': sat, 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version}
                for g in granules]

    def hirs_to_time_interval(self, filename):

        begin_time = datetime.strptime(filename[12:24], 'D%y%j.S%H%M')
        end_time = datetime.strptime(filename[12:19]+filename[25:30], 'D%y%j.E%H%M')
        if end_time < begin_time:
            end_time += timedelta(days=1)

        return TimeInterval(begin_time, end_time)

    def time_interval_to_hirs(self, interval):

        return '{}.{}'.format(interval.left.strftime('D%y%j.S%H%M'),
                              interval.right.strftime('E%H%M'))

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CSRB_DAILY')
