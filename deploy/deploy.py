
import os
from flo_deploy.packagelib import *

class HIRS_CSRB_Daily_Package(Package):

    def deploy_package(self):

        for version in ['v20140204']:
            self.merge(Extracted('HIRS_CSRB_Daily_Statistics_{}.tar.gz'.format(version)).path(), version) 
            self.merge(NetcdfFortran().path(), version)
            self.merge(Netcdf().path(), version)
            self.merge(Hdf5().path(), version)