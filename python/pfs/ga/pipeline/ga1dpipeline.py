import os
import time
import numpy as np

from pfs.datamodel import PfsConfig, PfsSingle
from pfs.ga.datamodel import PfsGAObject

from pfs.ga.pfsspec.core import Physics
from pfs.ga.pfsspec.core.obsmod.resampling import FluxConservingResampler, Interp1dResampler
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.snr import QuantileSnr
from pfs.ga.pfsspec.stellar.grid import ModelGrid
from pfs.ga.pfsspec.stellar.rvfit import ModelGridRVFit, ModelGridRVFitTrace

from .pipeline import Pipeline
from .pipelineerror import PipelineError
from .config import GA1DPipelineConfig
from .ga1dpipelinetrace import GA1DPipelineTrace
from .ga1dspectrum import GA1DSpectrum


class GA1DPipeline(Pipeline):
    """
    Implements the Galactic Archeology Spectrum Processing Pipeline.

    Inputs are the `PfsSingle` files of all individual exposures belonging to the same
    `objId` and all related pfsConfig files as dictionaries indexed by `visit`.

    The pipeline processes all exposures of a single object at a time and produces a
    PfsGAObject file which contains the measured parameters, their errors and full covariance
    matrix as well as a co-added spectrum, a continuum-normalized co-added spectrum and a
    best fit continuum model.

    The three main steps of the pipeline are i) line-of-sight velocity estimation,
    ii) abundance estimation and iii) spectrum stacking.
    """

    def __init__(self,
                 config: GA1DPipelineConfig,
                 trace: GA1DPipelineTrace = None,
                 pfsConfig: dict = None,
                 pfsSingle: dict = None):
        """
        Initializes a GA Pipeline object for processing of individual exposures of a
        single object.

        Parameters
        ----------
        config: :obj:`GA1DPipelineConfig`
            Configuration of the GA pipeline
        objId: :int:
            Unique object identifier
        pfsConfig: :dict:`int`,`PfsConfig`
            Dictionary of PfsConfig objects for each visit, keyed by ˙visit`.
        pfsSingle : :dict:`int`,`PfsSingle`
            Dictionary of PfsSingle object containing the individual exposures,
            keyed by ˙visit˙.
        """
        
        super().__init__(config=config, trace=trace)

        self._steps = [
            {
                'name': 'init',
                'func': self.__step_init,
                'critical': True
            },
            {
                'name': 'load',
                'func': self.__step_load,
                'critical': True
            },
            {
                'name': 'validate',
                'func': self.__step_validate,
                'critical': True
            },
            {
                'name': 'rvfit',
                'func': self.__step_rvfit,
                'critical': False
            },
            {
                'name': 'chemfit',
                'func': self.__step_chemfit,
                'critical': False
            },
            {
                'name': 'coadd',
                'func': self.__step_coadd,
                'critical': False
            },
            {
                'name': 'save',
                'func': self.__step_save,
                'critical': False
            },
            {
                'name': 'cleanup',
                'func': self.__step_cleanup,
                'critical': False
            },
        ]

        self.__pfsConfig = pfsConfig
        self.__pfsSingle = pfsSingle
        self.__idx = None                       # dict, index of object within pfsConfig files, for each visit
        
        self.__rvfit_res = None                 # Results from RVFit

        self.__pfsGAObject = None

    def __get_pfsGAObject(self):
        return self.__pfsGAObject
    
    pfsGAObject = property(__get_pfsGAObject)

    def _get_log_filename(self):
        return f'gapipe_{self.config.object.objId:016x}.log'

    def validate_config(self):
        """
        Validates the configuration and the existence of all necessary input data. Returns
        `True` if the pipeline can proceed or 'False' if it cannot.

        Return
        -------
        :bool:
            `True` if the pipeline can proceed or 'False' if it cannot.
        """

        if not os.path.isdir(self.config.workdir):
            raise FileNotFoundError(f'Working directory `{self.config.workdir}` does not exist.')
        
        if not os.path.isdir(self.config.datadir):
            raise FileNotFoundError(f'Data directory `{self.config.datadir}` does not exist.')

        if not os.path.isdir(os.path.join(self.config.datadir, self.config.rerundir)):
            raise FileNotFoundError(f'Rerun directory `{self.config.rerundir}` does not exist.')
        
        if self.config.run_rvfit:
            for arm in self.config.rvfit.arms:
                fn = self.config.rvfit.model_grid_path.format(arm=arm)
                if not os.path.isfile(fn):
                    raise FileNotFoundError(f'Synthetic spectrum grid `{fn}` not found.')
                
                if self.config.rvfit.psf_path is not None:
                    fn = self.config.rvfit.psf_path.format(arm=arm)
                    if not os.path.isfile(fn):
                        raise FileNotFoundError(f'PSF file `{fn}` not found.')

        return True
    
    def validate_libs(self):
        # TODO: write code to validate library versions and log git hash for each when available
        pass

    def __step_init(self):
        # Create output directories
        self._create_dir(self.config.outdir, 'output')
        self._create_dir(self.config.figdir, 'figure')

    def __step_load(self):
        # Load each PfsConfig and PfsSingle file.

        # TODO: skip loading files if the object are already passed to the pipeline
        #       from the outside

        if self.config.load_pfsConfig:
            self.__pfsConfig = {}
        else:
            self.__pfsConfig = None

        self.__pfsSingle = {}

        start_time = time.perf_counter()

        for i, visit in enumerate(self.config.object.visits.keys()):
            identity = {
                'objId': self.config.object.objId,
                'catId': self.config.object.catId,
                'tract': self.config.object.tract,
                'patch': self.config.object.patch,
                'visit': visit
            }

            if self.config.load_pfsConfig:
                self.__pfsConfig[visit] = self.__load_pfsConfig(visit)
            
            self.__pfsSingle[visit] = self.__load_pfsSingle(identity)

        stop_time = time.perf_counter()
        self.logger.info(f'PFS data files loaded successfully for {len(self.__pfsSingle)} exposures in {stop_time - start_time:.3f} s.')

    def __load_pfsConfig(self, visit):
        pfsDesignId = self.config.object.visits[visit].pfsDesignId
        date = self.config.object.visits[visit].date

        dir = os.path.join(self.config.datadir, 'pfsConfig/{date}'.format(date=date))
        fn = PfsConfig.fileNameFormat % (pfsDesignId, visit)

        self.logger.info(f'Loading PfsConfig from `{os.path.join(dir, fn)}`.')
        
        start_time = time.perf_counter()
        pfsConfig =  PfsConfig.read(pfsDesignId, visit, dirName=dir)
        stop_time = time.perf_counter()

        self.logger.info(f'Loaded PfsConfig from `{os.path.join(dir, fn)}` in {stop_time - start_time:.3f} s.')

        return pfsConfig

    def __load_pfsSingle(self, identity):
        dir = os.path.join(self.config.datadir,
                           self.config.rerundir,
                           'pfsSingle/{catId:05d}/{tract:05d}/{patch}'.format(**identity))
        fn = PfsSingle.filenameFormat % identity
        
        self.logger.info(f'Loading PfsSingle from `{os.path.join(dir, fn)}`.')

        start_time = time.perf_counter()
        pfsSingle = PfsSingle.read(identity, dirName=dir)
        stop_time = time.perf_counter()

        self.logger.info(f'Loaded PfsSingle from `{os.path.join(dir, fn)}` in {stop_time - start_time:.3f} s.')

        return pfsSingle
    
    def __step_validate(self):
        # Extract info from pfsConfig and pfsSingle objects one by one and perform
        # some validation steps

        self.__idx = {}
        self.__identity = {}

        for visit in self.__pfsSingle.keys():
            if self.__pfsConfig is not None:
                pfsConfig = self.__pfsConfig[visit]
                self.__validate_pfsConfig(visit, pfsConfig)

            pfsSingle = self.__pfsSingle[visit]
            self.__validate_pfsSingle(visit, pfsSingle)

        # TODO: Make sure that coordinates are the same
            
        # TODO: Count spectra per arm and write report to log

    def __validate_pfsConfig(self, visit, pfsConfig):
        # Verify that visit numbers match
        if pfsConfig.visit != visit:
            raise PipelineError(f'Visit does not match visit ID found in `{pfsConfig.filename}`')

        # Verify that object ID is found in pfsConfig
        idx = np.where(pfsConfig.objId == self.config.object.objId)[0]
        if idx.size == 0:
            raise PipelineError(f'Object ID `{self.config.object.objId}` not found in `{pfsConfig.filename}`.')
        elif idx.size > 1:
            raise PipelineError(f'Object ID `{self.config.object.objId}` found more than once in `{pfsConfig.filename}`.')
        
        self.__idx[visit] = idx[0]
    
    def __validate_pfsSingle(self, visit, pfsSingle):
        fn = pfsSingle.filenameFormat % {**pfsSingle.getIdentity(), 'visit': visit}

        # Verify that it is a single visit and not a co-add
        if pfsSingle.nVisit != 1:
            raise PipelineError('More than one visit found in `{pfsSingle.filename}`')
        
        # Verify that visit numbers match
        if visit not in pfsSingle.observations.visit:
            raise PipelineError(f'Visit does not match visit ID found in `{fn}`.')
        
        if pfsSingle.target.catId != self.config.object.catId:
            raise PipelineError(f'catId in config `{self.config.object.catId}` does not match catID in `{fn}`.')

        if pfsSingle.target.objId != self.config.object.objId:
            raise PipelineError(f'objId in config `{self.config.object.objId}` does not match objID in `{fn}`.')
        
    #region RVFIT
        
    def __step_rvfit(self):
        avail_arms = self.__rvfit_get_avail_arms()
        fit_arms = self.__rvfit_validate(avail_arms)

        template_grids = self.__rvfit_load_grid(fit_arms)
        template_psfs = self.__rvfit_load_psf(fit_arms, template_grids)
        rvfit = self.__rvfit_init(template_grids, template_psfs)

        # Extract the spectra of individual arms from the pfsSingle objects
        spectra = self.__rvfit_load_spectra(fit_arms)

        # Determine the normalization factor to be used to keep continuum coefficients unity
        rvfit.spec_norm, rvfit.temp_norm = rvfit.get_normalization(spectra)

        # Run the maximum likelihood fitting
        self.__rvfit_res = rvfit.fit_rv(spectra)
    
    def __rvfit_get_avail_arms(self):
        # TODO: add option to require that all observations contain all arms

        # Collect all arms available in observations

        arms = set()
        for visit, pfsSingle in self.__pfsSingle.items():
            for arm in pfsSingle.observations.arm[0]:
                if arm not in arms:
                    arms.add(arm)

        return arms
    
    def __rvfit_validate(self, avail_arms):
        # Find a unique set of available arms in the pfsSingle files

        # NOTE: for some reason, pfsSingle.observations.arm is a char array that contains a single string
        #       in item 0 and not an array of characters

        # Verify that all arms defined in the config are available
        fit_arms = set()
        for arm in self.config.rvfit.fit_arms:
            if self.config.rvfit.require_all_arms and arm not in avail_arms:
                raise PipelineError(f'RVFIT requires arm `{arm}` which is not observed.')
            
            if arm in avail_arms:
                fit_arms.add(arm)

        return fit_arms
    
    def __rvfit_load_grid(self, arms):
        # Load template grids. Make sure each grid is only loaded once, if grid is
        # independent of arm.

        grids = {}
        for arm in arms:
            fn = self.config.rvfit.model_grid_path.format(arm)
            skip = False
            for _, ˇgrid in grids.items():
                if ˇgrid.filename == fn:
                    grids[arm] = ˇgrid
                    skip = True
                    break

            if not skip:
                grid = ModelGrid.from_file(fn, 
                                           preload_arrays=self.config.rvfit.model_grid_preload,
                                           mmap_arrays=self.config.rvfit.model_grid_mmap, 
                                           args=self.config.rvfit.model_grid_args,
                                           slice_from_args=False)
                if grid.wave_edges is None:
                    grid.wave_edges = FluxConservingResampler().find_wave_edges(grid.wave)

                grids[arm] = grid

        return grids
    
    def __rvfit_load_psf(self, arms, grids):
        # Right now load a PSF file generate by the ETC        
        # TODO: Modify this to use PSF from 2D pipeline instead of ETC
        
        psfs = {}
        for arm in arms:
            fn = self.config.rvfit.psf_path.format(arm=arm)
            gauss_psf = GaussPsf()
            gauss_psf.load(fn)

            if grids is not None:
                wave = grids[arm].wave
            else:
                raise NotImplementedError()

            s = gauss_psf.get_optimal_size(wave)
            self.logger.info(f'Optimal kernel size for PSF in arm `{arm}` is {s}.')

            pca_psf = PcaPsf.from_psf(gauss_psf, wave, size=s, truncate=5)
            psfs[arm] = pca_psf

        return psfs
    
    def __rvfit_init(self, template_grids, template_psfs):
        trace = ModelGridRVFitTrace(self.config.figdir)
        trace.figure_formats = [ '.pdf', '.png' ]
        
        rvfit = ModelGridRVFit(trace)

        rvfit.template_grids = template_grids
        rvfit.template_psf = template_psfs

        rvfit.init_from_args(None, None, self.config.rvfit.rvfit_args)
        rvfit.trace.init_from_args(None, None, self.config.rvfit.trace_args)

        return rvfit
    
    def __rvfit_load_spectra(self, arms):
        # Extract spectra from the fluxtables of pfsSingle objects

        spectra = { arm: [] for arm in arms }

        for i, visit in enumerate(sorted(self.__pfsSingle.keys())):
            pfsConfig = self.__pfsConfig[visit]
            pfsSingle = self.__pfsSingle[visit]

            # nm -> A            
            wave = Physics.nm_to_angstrom(pfsSingle.fluxTable.wavelength)

            # nJy -> erg s-1 cm-2 A-1
            flux = 1e-32 * Physics.fnu_to_flam(wave, pfsSingle.fluxTable.flux)
            flux_err = 1e-32 * Physics.fnu_to_flam(wave, pfsSingle.fluxTable.error)

            # TODO: add logic to accept some masked pixels if the number of unmasked pixels is low

            mask = (pfsSingle.fluxTable.mask == 0)

            # Slice up by arm
            for arm in arms:
                armmask = (self.config.rvfit.arms[arm]['wave'][0] <= wave) & (wave <= self.config.rvfit.arms[arm]['wave'][1])
                
                s = GA1DSpectrum()
                s.index = i
                s.catId = self.config.object.catId
                s.objId = s.id = self.config.object.objId
                s.visit = visit

                s.spectrograph = pfsConfig.spectrograph[self.__idx[visit]]
                s.fiberid = pfsConfig.fiberId[self.__idx[visit]]

                s.wave = wave[armmask]
                s.flux = flux[armmask]
                s.flux_err = flux_err[armmask]
                s.mask = mask[armmask]

                # SNR
                s.calculate_snr(QuantileSnr(0.75, binning=self.config.rvfit.arms[arm]['pix_per_res']))

                # Target PSF magnitude from pfsConfig metadata
                filters = pfsConfig.filterNames[self.__idx[visit]]
                if self.config.rvfit.ref_mag in filters:
                    fidx = filters.index(self.config.rvfit.ref_mag)

                    # TODO: convert nJy to ABmag
                    s.mag = pfsConfig.psfFlux[self.__idx[visit]][fidx]
                else:
                    s.mag = np.nan

                # TODO: where do we take these from?
                # s.exp_count = 1
                # s.exp_time = 0
                # s.seeing = 0
               
                spectra[arm].append(s)

        if self.trace is not None:
            self.trace.on_rvfit_load_spectra(spectra)

        return spectra

    def __rvfit_cleanup(self):
        pass

    #endregion
    #region CHEMFIT

    def __step_chemfit(self):
        # TODO: run abundance fitting
        raise NotImplementedError()
    
    #endregion

    def __step_coadd(self):
        # TODO: run spectrum co-adding
        raise NotImplementedError()

    def __step_save(self):
        raise NotImplementedError()

        # Construct the output object based on the results from the pipeline steps
        target = self.__create_target()

        self.__pfsGAObject = PfsGAObject(target, observations,
                                  wavelength, flux, mask, sky, covar, covar2,
                                  flags, metadata,
                                  fluxTable,
                                  stellarParams,
                                  velocityCorrections,
                                  abundances,
                                  paramsCovar,
                                  abundCovar,
                                  notes)

        # TODO: save any outputs
        raise NotImplementedError()
    
    def __create_target(self):
        # Construct the target object
        pass

    def __step_cleanup(self):
        # TODO: Perform any cleanup
        raise NotImplementedError()
