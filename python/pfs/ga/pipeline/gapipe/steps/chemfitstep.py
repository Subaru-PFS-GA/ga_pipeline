import os
import numpy as np

from ...setup_logger import logger

import pfs.datamodel
from pfs.datamodel import *

try:
    from chemfit import ChemFit, LocalFit, LocalGrid
except ImportError as e:
    logger.warning(f'Failed to import chemfit: {e}')
    ChemFit = None
    LocalFit = None
    LocalGrid = None

from pfs.ga.pfsspec.stellar import StellarSpectrum
from pfs.ga.common.util.astro import vel_to_z
from ...chemfit import ChemFitResults
from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

class ChemFitStep(PipelineStep):

    # Map TempFit parameters to ChemFit parameters
    # Not all of these are actually used by ChemFit
    TEMPFIT_PARAM_MAP = {
        'M_H': 'zscale',
        'T_eff': 'teff',
        'log_g': 'logg',
        'a_M': 'alpha',
        'C': 'carbon',
        'ebv': 'ebv',
        'v_los': 'z'
    }
    
    # Map ChemFit parameters to TempFit parameters
    CHEMFIT_PARAM_MAP = {v: k for k, v in TEMPFIT_PARAM_MAP.items()}

    # List of params that ChemFit.gridfit accepts
    ALLOWED_GRIDFIT_PARAMS = ['teff', 'logg', 'carbon']

    def __init__(self, name=None):
        super().__init__(name)

    def init(self, context):
        if not context.config.run_chemfit:
            logger.info('Chemical abundance fitting is disabled, skipping...')
            return PipelineStepResults(success=True, skip_remaining=True, skip_substeps=True)

        # Depending on the configuration, we may want to use the coadded spectrum
        # or the individual exposures
        if context.config.chemfit.fit_coadd:
            if not context.config.run_coadd:
                logger.info('Spectrum stacking is disabled, skipping step.')
                return PipelineStepResults(success=True, skip_remaining=True, skip_substeps=True)

            # Use the coadded spectrum to find the available arms for chemical fitting
            avail_arms = set(context.state.coadd_results.coadd_spectra.keys())
        else:
            # Use the individual exposures to find the available arms for chemical fitting
            avail_arms = set(context.state.tempfit_spectra.keys())
        
        logger.info(f'Available arms for ChemFit: {sorted(avail_arms)}')

        # Verify that all arms required in the config are available
        context.state.chemfit_arms = set()
        for arm in context.config.chemfit.fit_arms:
            message = f'ChemFit requires arm `{arm}` which is not observed.'
            if context.config.chemfit.require_all_arms and arm not in avail_arms:
                raise PipelineError(message)
            elif arm not in avail_arms:
                logger.warning(message)
            else:
                context.state.chemfit_arms.add(arm)
                
        logger.info(f'Using arms for ChemFit: {sorted(context.state.chemfit_arms)}')

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def __calculate_jacobian(self, context):

        """
        Calculate the Jacobian of the flux with respect to the atmospheric parameters using TempFit.
        """

        # TODO: this is very similar to what's happening in CoaddStep.__fit_coadd_spectra

        tempfit = context.state.tempfit
        tempfit.reset()

        tempfit_state = tempfit.init_state(
            context.state.coadd_results.coadd_spectra,
            rv_fixed = context.state.tempfit_state.rv_fixed,
            params_fixed = context.state.tempfit_state.params_fixed,
            fluxes = context.state.tempfit_fluxes)

        # Copy best fit parameters from likelihood stacking results        
        tempfit_state.rv_fit = context.state.tempfit_results.rv_fit
        tempfit_state.params_fit = context.state.tempfit_results.params_fit.copy()
        tempfit_state.a_fit = context.state.tempfit_results.a_fit

        # Initialize the correction models and extinction curves to match
        # the wavelength grid of the coadded spectra
        tempfit.init_correction_models(tempfit_state.pp_spec, force=True)
        tempfit.init_extinction_curves(tempfit_state.pp_spec, force=True)

        # Calculate the Jacobian needed for the final covariance matrix in ChemFit
        tempfit_results, tempfit_state = tempfit.calculate_jac_ml(
            tempfit_state,
            normalize_continuum = context.config.chemfit.normalize_continuum
        )

        # Chemfit's localfit expects the Jacobian in teff, logg and carbon only
        # Keep only the relevant rows of the Jacobian
        jac = tempfit_results.jac
        jac_params = []
        if jac is not None:
            keep = []
            for i, p in enumerate(tempfit_results.jac_params):
                cp = self.TEMPFIT_PARAM_MAP.get(p, None)
                if cp in self.ALLOWED_GRIDFIT_PARAMS:
                    jac_params.append(cp)
                    keep.append(i)

            jac = jac[:, keep]
        else:
            jac_params = []
            jac = None

        return jac, jac_params

    def __convert_spectra(self, context):
        wl = {}
        flux = {}
        ivar = {}
        for arm in context.state.chemfit_arms:
            # Coadds have a single spectrum only for each arm
            s = context.state.coadd_results.coadd_spectra[arm][0].copy()
            z = vel_to_z(context.state.tempfit_results.rv_fit)
            
            # Create a mask to pass in good pixels only. ChemFit cannot take a
            # simple bitmask, so we will simply set the flux to NaN for the masked
            # pixels but pass in all wavelength values, otherwise it would be difficult
            # to retreive the final model from ChemFit.localfit.
            mask_bits = s.get_mask_bits(context.config.chemfit.mask_flags)
            mask = s.mask_as_bool(bits=mask_bits)
            mask = mask if mask is not None else np.full_like(s.wave, True, dtype=bool)

            # TODO: apply the chemfit mask in extra, otherwise the
            #       shape of the Jacobian will be different
            # context.config.chemfit.settings['masks']['rest'] + context.config.chemfit.settings['masks']['lab'],

            # Correct for extinction
            ebv = context.state.coadd_tempfit_results.params_fit.get('ebv', None)
            if ebv is not None:
                s.correct_extinction(ebv=ebv)

            # Convert to air wavelengths, chemfit cannot work in vacuum wavelengths
            s.vac_to_air()

            # Convert to rest-frame wavelengths, use full wavelength array
            wl[arm] = s.wave / (1 + z)

            # Normalize the observed spectrum with the continuum, when available
            if context.config.chemfit.normalize_continuum and s.line is not None:
                flux[arm] = np.where(mask, s.line[mask], np.nan)
                ivar[arm] = 1.0 / np.where(mask, s.line[mask], np.nan) ** 2 if s.line is not None else None
            elif context.config.chemfit.normalize_continuum and s.cont is not None:
                flux[arm] = np.where(mask, s.flux / s.cont, np.nan)
                ivar[arm] = 1.0 / np.where(mask, s.flux_err / s.cont, np.nan) ** 2 if s.flux_err is not None else None
            elif context.config.chemfit.normalize_continuum:
                raise ValueError("Cannot normalize continuum: no line or continuum available in the spectrum.")
            else:
                flux[arm] = np.where(mask, s.flux, np.nan)
                ivar[arm] = 1.0 / np.where(mask, s.flux_err, np.nan) ** 2 if s.flux_err is not None else None

        return wl, flux, ivar

    def __convert_tempfit_params(self, context):
        gridfit_params = {}
        for key in context.state.tempfit_results.params_fit:
            if key in self.TEMPFIT_PARAM_MAP:
                value = context.state.tempfit_results.params_fit[key]
                error = context.state.tempfit_results.params_err[key]
                gridfit_params[self.TEMPFIT_PARAM_MAP[key]] = (value, error)

        return gridfit_params

    def __mimic_gridfit_results(
        self,
        context,
        jac, jac_params,
        wl, flux, ivar,
        gridfit_params
    ):

        """
        Create a dictionary that mimics the structure of gridfit results for ChemFit.
        """
        
        gridfit_results = {
            'localfit': {
                'wl': wl,
                'flux': flux,
                'ivar': ivar,

                # NOTE: this would be the mask ranges, converted into the rest-frame but
                #       we don't apply a mask in chemfit, we simply don't pass the masked
                #       pixels to it.
                'mask': [],

                'jacobian': [ jac_params, jac ],
                'gridfit': gridfit_params
            }
        }

        return gridfit_results

    def run(self, context):
        # ChemFit.localfit expects the Jacobian of the flux with respect to the
        # atmospheric parameters. Tempfit does not, by default, provide it becase
        # we calculate the full Hessian instead to estimate the covariances.
        jac, jac_params = self.__calculate_jacobian(context)

        # Convert pfsspec spectra to chemfit format
        wl, flux, ivar = self.__convert_spectra(context)

        # Convert the best-fit TempFit parameters to ChemFit parameters
        gridfit_params = self.__convert_tempfit_params(context)

        # Create a dictionary that mimics the structure of gridfit results.
        # This will be used to pass directly to ChemFit.localfit and prevent
        # re-calculating the gridfit results from scratch.
        gridfit_results = self.__mimic_gridfit_results(
            context,
            jac, jac_params,
            wl, flux, ivar,
            gridfit_params
        )
        
        # Initialize a localfit object using the settings from the gapipe config
        localfit = LocalFit()
        localfit.settings = context.config.chemfit.settings

        # Create the LocalGrid object that runs ATLAS on the fly
        # TODO: pass in the model grid wrapper to reuse the pfsspec interpolator
        #       instead of ChemFit's
        grid = LocalGrid(localfit)
        localfit.grid = grid
        grid.gridfit = localfit

        # Override the list of atmospheric parameters that are allowed to vary
        # during ChemFit.
        # TODO: maybe allow log g?
        # TODO: specifying the offset will raise an exception at localfit.py:271
        #       "The best-fit value of parameter logg has been updated during the fit.
        #       The provided uncertainty in this parameter therefore cannot be used"
        #       Looks like we just have to make sure the error of the tempfit parameter
        #       is not passed in -- error should be replaced with warning?
        # localfit.settings['gridfit_offsets'] = { 'logg': [-1.0, -0.5, 0.0, 0.5, 1.0] }
        localfit.settings['gridfit_offsets'] = { }

        # TODO: remove this after debugging
        # Override element list to speed up the test.
        # Keep all settings derived from the element list in sync.
        elements = ['Mg', 'Si', 'Ca']
        localfit.settings['virtual_dof'] = {
            elem: localfit.settings['virtual_dof'][elem]
            for elem in elements
        }
        localfit.settings['elements'] = {
            elem: localfit.settings['elements'][elem]
            for elem in elements
        }
        localfit.settings['default_initial'] = {
            elem: localfit.settings['default_initial'][elem]
            for elem in elements
        }

        # TODO: verify scratch location, it seems to take it from local settings

        # TODO: figure out how to monkey-patch convolution,
        #       probably replace simulate_observation entirely

        # TODO: figure out how to save arrays into yaml
        localfit.settings['abun'] = np.array(localfit.settings['abun'])

        # TODO: figure out how to knock-out the continuum finder

        # TODO: allow higher levels

        # Run abundance fitting
        localfit_results = localfit.localfit(**gridfit_results['localfit'], level = 1)

        # TODO: extract results

        # localfit_results['fit']['teff'] etc
        # localfit_results['fit']['[Mg/H]']
        # localfit_results['fit']['[Ca/H]'] etc.

        # localfit_results['errors']['[Mg/H]']
        # localfit_results['errors']['[Ca/H]'] etc.

        # localfit_results['extra']['dof']
        # localfit_results['extra']['cov'] # includes jac_dof and the fitted elements

        # localfit_results['extra']['observed'] # original spectrum
        # localfit_results['extra']['mask'] # will be all true
        # localfit_results['extra']['arm_index'] # 0, 1, 2... for each arm in flux array

        # localfit_results['extra']['intermediate'] = [{'[Mg/H]': np.float64(-2.9126888299705067), '[Si/H]': np.float64(-2.9126888299705067), '[Ca/H]': np.float64(-2.912688829970468)}]
        
        # Get the best fit chemfit model

        # localfit_results['extra']['model']['wl']
        # localfit_results['extra']['model']['flux'] -- this is the model flux but it's not scaled to the observed spectrum
        # localfit_results['extra']['model']['cont'] -- this is the fitted continuum, not the model continuum
        #                                               flux * cont seems to be the best fit to the data

        # Extract the best fit abundances and errors
        # 

        chemfit_spectra = {}
        sorted_arms = sorted(list(context.state.chemfit_arms))
        for arm in gridfit_results['localfit']['wl']:
            arm_mask = localfit_results['extra']['arm_index'] == sorted_arms.index(arm)
            flux = localfit_results['extra']['model']['flux'][arm_mask]
            cont = localfit_results['extra']['model']['cont'][arm_mask]     # This is not the model continuum
        
            s = StellarSpectrum()
            s.wave = context.state.coadd_results.coadd_spectra[arm][0].wave.copy()
            s.flux_model = flux * cont
            # s.cont = 
            # s.line_model =

            # Since we passed an extinction-corrected spectrum to ChemFit, now apply it to
            # the model to match the non-corrected flux
            s.apply_extinction(
                curve = context.state.coadd_results.coadd_spectra[arm][0].flux_ext,
                ebv = context.state.tempfit_results.params_fit['ebv']
            )

            chemfit_spectra[arm] = [s]

        # Figure out the free parameters for the covariance matrix
        cov_params = []
        for p in localfit_results['extra']['dof']:
            if p in self.CHEMFIT_PARAM_MAP:
                # It's an atmospheric parameter
                cov_params.append(self.CHEMFIT_PARAM_MAP[p])
            else:
                # It's an abundance, get name of element from [Xx/H] notation
                cov_params.append(p[1:p.index('/')])
        
        chemfit_results = ChemFitResults(
            params_free = [ self.CHEMFIT_PARAM_MAP[p] for p in localfit.settings['gridfit_offsets'] ],
            params_fit = { localfit_results['fit'][p] for p in localfit.settings['gridfit_offsets'] },
            params_err = { localfit_results['errors'][p] for p in localfit.settings['gridfit_offsets'] },

            abund_free = [ p for p in localfit.settings['elements'] ],
            abund_fit = { p: localfit_results['fit'][f'[{p}/H]'] for p in localfit.settings['elements'] },
            abund_err = { p: localfit_results['errors'][f'[{p}/H]'] for p in localfit.settings['elements'] },

            jac = localfit_results['extra']['jac'],
            jac_params = [],
            
            cov = localfit_results['extra']['cov'],
            cov_params = cov_params,
        )

        # TODO: flag params on the edge, etc

        # Store results
        context.state.chemfit_results = chemfit_results

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def cleanup(self, context):

        # TODO: Clean up any temporary files or directories created during the ChemFit step

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)