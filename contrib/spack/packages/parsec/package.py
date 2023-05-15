##############################################################################
# Copyright (c) 2017-2019  The University of Tennessee and the University
#                          of Tennessee Research Foundation.  All rights
#                          reserved.
#
# $COPYRIGHT$
#
##############################################################################
from spack import *
import os

class Parsec(CMakePackage):
    """PaRSEC: the Parallel Runtime Scheduler and Execution Controller for micro-tasks on distributed heterogeneous systems"""

    homepage = "https://bitbucket.org/icldistcomp/parsec"
    git      = "https://bitbucket.org/icldistcomp/parsec.git"
    url      = "https://bitbucket.org/icldistcomp/parsec/get/v1.1.0.tar.bz2"

    version('devel', branch='master')
    version('devel-profiling', commit='9097f88')
    patch('profile2h5.patch', when='@devel-profiling')
    version('1.1.0', '6c8b2b8d6408004bdb4c6d9134da74a4')

    variant('transport', default='mpi', values=('mpi', 'lci', 'none'), multi=False,
            description='Inter-node dependency transport backend')
    variant('lci-static', default=False, description='Static link to LCI')
    variant('lci-msg-limit', default=False, description='Limit concurrent messages')
    variant('debug-lci', default=False, description='Enable LCI CE debug')
    variant('lci-cbtable', default=False, description='Use callback hashtable')
    variant('cuda', default=True, description='Use CUDA for GPU acceleration')
    variant('profile', default=False, description='Generate profiling data')
    variant('profile-with-mmap', default=True, description='Use mmap for profiling')
    variant('profile-tools', default=True, description='Install profiling tools')
    variant('debug', default=False, description='Debug version (incurs performance overhead!)')
    variant('debug-history', default=False, description='Debug history (for GDB dumping)')
    #variant('xcompile', default=False, description='Cross compile')
    variant('visualization', default=False, description='Visualization support')
    variant('graph', default=False, description='Graphing support')
    variant('stats', description='Lightweight timing statistics',
            values=any_combination_of('sched', 'comm', 'tc'))

    conflicts('+lci-static', when='transport=mpi')
    conflicts('+debug-lci', when='transport=mpi')
    conflicts('+lci-cbtable', when='transport=mpi')
    conflicts('transport=mpi', when='^lci')
    conflicts('transport=lci', when='^mpi')

    generator = 'Ninja'
    depends_on('ninja', type='build')

    depends_on('cmake@3.16.0:', type='build')
    depends_on('bison', type='build')
    depends_on('flex', type='build')
    depends_on('hwloc')
    depends_on('mpi', when='transport=mpi')
    depends_on('lci', when='transport=lci')
    depends_on('cuda', when='+cuda')
    depends_on('papi', when='+profile')
    depends_on('libxml2', when='+visualization')
    depends_on('graphviz', when='+graph')
    depends_on('libgd', when='+graph')

    # profiling tools install python modules and have several python deps
    extends('python',           when='+profile-tools')
    depends_on('python',        when='+profile-tools', type=('build', 'run'))
    depends_on('py-cython',     when='+profile-tools', type='build')
    depends_on('py-numpy',      when='+profile-tools', type=('build', 'run'))
    depends_on('py-pandas',     when='+profile-tools', type=('build', 'run'))
    depends_on('py-matplotlib', when='+profile-tools', type=('build', 'run'))
    depends_on('py-networkx',   when='+profile-tools', type=('build', 'run'))
    # py-pandas HDF5 support
    depends_on('py-tables',     when='+profile-tools', type='run')

    @property
    def build_directory(self):
        return os.path.join(self.stage.path, 'spack-build', self.spec.dag_hash(7))

    def cmake_args(self):
        args = [
            self.define_from_variant('PARSEC_DEBUG_HISTORY', 'debug-history'),
            self.define_from_variant('PARSEC_DEBUG_PARANOID', 'debug'),
            self.define_from_variant('PARSEC_DEBUG_NOISIER', 'debug'),
            self.define_from_variant('PARSEC_GPU_WITH_CUDA', 'cuda'),
            self.define_from_variant('PARSEC_PROF_TRACE', 'profile'),
            self.define_from_variant('PARSEC_PROFILING_USE_MMAP', 'profile-with-mmap'),
            self.define('PARSEC_DIST_WITH_MPI', self.spec.variants['transport'].value == 'mpi'),
            self.define('PARSEC_DIST_WITH_LCI', self.spec.variants['transport'].value == 'lci'),
            self.define_from_variant('LCI_LINK_STATIC', 'lci-static'),
            self.define_from_variant('PARSEC_LCI_RETRY_HISTOGRAM', 'debug-lci'),
            self.define_from_variant('PARSEC_LCI_HANDLER_COUNT', 'debug-lci'),
            self.define_from_variant('PARSEC_LCI_CB_HASH_TABLE', 'lci-cbtable'),
            self.define_from_variant('PARSEC_LCI_MESSAGE_LIMIT', 'lci-msg-limit'),
            self.define('PARSEC_DIST_COLLECTIVES', True),
            self.define('PARSEC_STATS', 'none' not in self.spec.variants['stats'].value),
            self.define('PARSEC_STATS_SCHED', 'sched' in self.spec.variants['stats'].value),
            self.define('PARSEC_STATS_COMM', 'comm' in self.spec.variants['stats'].value),
            self.define('PARSEC_STATS_TC', 'tc' in self.spec.variants['stats'].value),
        ]
        # lci uses a default eager limit of 12 KiB - 108 bytes
        if self.spec.variants['transport'].value == 'lci':
            args.append(self.define('PARSEC_DIST_EAGER_LIMIT', '12180'))

        return args
