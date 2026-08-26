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

from pfs.ga.common.util.astro import vel_to_z
from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

class ChemFitStep(PipelineStep):
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

    def run(self, context):

        # Map TempFit parameters to ChemFit parameters and back
        tempfit_param_map = {
            'M_H': 'zscale',
            'T_eff': 'teff',
            'log_g': 'logg',
            'a_M': 'alpha',
            'C': 'carbon',
            'ebv': 'ebv',
            'v_los': 'z'
        }
        chemfit_param_map = {v: k for k, v in tempfit_param_map.items()}

        # Chemfit's localfit expects the Jacobian in teff, logg and carbon only
        # Keep only the relevant rows of the Jacobian
        allowed_chemfit_cov_params = ['T_eff', 'log_g', 'C']
        jac = context.state.coadd_tempfit_results.jac
        jac_params = []
        if jac is not None:
            keep = []
            for i, p in enumerate(context.state.coadd_tempfit_results.jac_params):
                if p in allowed_chemfit_cov_params:
                    jac_params.append(tempfit_param_map[p])
                    keep.append(i)

            # TODO: this jacobian is for the flux and not the continuum-normalized flux
            #       modify this if localfit is used to fit the normalized spectrum
            jac = jac[:, keep]
        else:
            jac_params = []
            jac = None

        gridfit_results = {
            'localfit': {
                'wl': {},
                'flux': {},
                'ivar': {},

                # NOTE: this would be the mask ranges, converted into the rest-frame but
                #       we don't apply a mask in chemfit, we simply don't pass the masked
                #       pixels to it.
                'mask': [],

                'jacobian': [ jac_params, jac ],
                'gridfit': {}
            }
        }

        # Convert pfsspec spectra to chemfit format
        spectrum = {}
        for arm in context.state.chemfit_arms:
            s = context.state.coadd_results.coadd_spectra[arm][0]
            z = vel_to_z(context.state.tempfit_results.rv_fit)
            
            # Only pass in the good pixels.
            mask_bits = s.get_mask_bits(context.config.chemfit.mask_flags)
            mask = s.mask_as_bool(bits=mask_bits)
            mask = mask if mask is not None else np.s_[:]

            # TODO: apply the chemfit mask in extra, otherwise the
            #       shape of the Jacobian will be different
            # context.config.chemfit.settings['masks']['rest'] + context.config.chemfit.settings['masks']['lab'],

            # Convert to rest-frame wavelengths
            gridfit_results['localfit']['wl'][arm] = s.wave[mask] * (1 + z)

            # Normalize the observed spectrum with the continuum, when available
            # if s.line is not None:
            #     gridfit_results['localfit']['flux'][arm] = s.line[mask]
            #     gridfit_results['localfit']['ivar'][arm] = 1.0 / s.line[mask] ** 2 if s.line is not None else None
            # elif s.cont is not None:
            #     gridfit_results['localfit']['flux'][arm] = s.flux[mask] / s.cont[mask]
            #     gridfit_results['localfit']['ivar'][arm] = 1.0 / (s.flux_err[mask] / s.cont[mask]) ** 2 if s.flux_err is not None else None
            # else:
            #     gridfit_results['localfit']['flux'][arm] = s.flux[mask]
            #     gridfit_results['localfit']['ivar'][arm] = 1.0 / s.flux_err[mask] ** 2 if s.flux_err is not None else None

            gridfit_results['localfit']['flux'][arm] = s.flux[mask]
            gridfit_results['localfit']['ivar'][arm] = 1.0 / s.flux_err[mask] ** 2 if s.flux_err is not None else None
            
        # Convert the best-fit TempFit parameters to ChemFit parameters
        for key in context.state.tempfit_results.params_fit:
            if key in tempfit_param_map:
                value = context.state.tempfit_results.params_fit[key]
                error = context.state.tempfit_results.params_err[key]
                gridfit_results['localfit']['gridfit'][tempfit_param_map[key]] = (value, error)
        
        # Initialize a localfit object using the settings from the gapipe config
        localfit = LocalFit()
        localfit.settings = context.config.chemfit.settings

        # Create the LocalGrid object that runs ATLAS on the fly
        grid = LocalGrid(localfit)
        localfit.grid = grid
        grid.gridfit = localfit

        # Override some default settings
        # TODO: maybe allow log g?
        localfit.settings['gridfit_offsets'] = {}

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

        # TODO: calculate the jacobian for the tempfit parameters
        #       shape should be [wave, param]

        # Run abundance fitting
        localfit_results = localfit.localfit(**gridfit_results['localfit'], level = 1)

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
        
        # It would be great to have this for all wavelength
        # consider using a mask instead of just passing in the unmasked pixels

        # Flux will need to be scaled to the spectrum

        # localfit_results['extra']['model']['wl']
        # localfit_results['extra']['model']['flux'] -- this is the model flux but it's not scaled to the observed spectrum
        # localfit_results['extra']['model']['cont'] -- this is the fitted continuum, not the model continuum
        #                                               flux * cont seems to be the best fit to the data

        # TODO: figure out how to knock-out the continuum finder
        
        # TODO: save results
        raise NotImplementedError('ChemFit results saving is not yet implemented.')

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def cleanup(self, context):
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)