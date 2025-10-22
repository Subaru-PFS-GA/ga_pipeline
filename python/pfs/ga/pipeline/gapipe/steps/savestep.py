import os
import numpy as np

import pfs.datamodel
from pfs.datamodel import *

from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

from ...setup_logger import logger

class SaveStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    def run(self, context):

        state = context.state
        merged_spectrum = state.coadd_results.merged_spectrum

        # Construct the output object based on the results from the pipeline steps
        # TODO: 
        metadata = {}

        flags = MaskHelper(**{ v: k for k, v in merged_spectrum.mask_flags.items() })

        # Construct the flux table, this is an alternative representation of the spectrum
        shape = merged_spectrum.wave.shape
        
        flux_model = merged_spectrum.flux_model if merged_spectrum.flux_model is not None else np.zeros(shape)
        cont = merged_spectrum.cont if merged_spectrum.cont is not None else np.zeros(shape)
        norm_flux = merged_spectrum.flux / merged_spectrum.cont if merged_spectrum.cont is not None else np.zeros(shape)
        norm_err = merged_spectrum.flux_err / merged_spectrum.cont if merged_spectrum.cont is not None else np.zeros(shape)
        norm_model = merged_spectrum.flux_model / merged_spectrum.cont if (merged_spectrum.flux_model is not None and merged_spectrum.cont is not None) else np.zeros(shape)

        flux_table = StarFluxTable(
            merged_spectrum.wave,
            merged_spectrum.flux,
            merged_spectrum.flux_err,
            flux_model,                 # Best-fit fluxed model
            cont,                       # Model continuum
            norm_flux,                  # Continuum-normalized flux
            norm_err,                   # Error of continuum-normalized flux
            norm_model,                 # Continuum-normalized model
            merged_spectrum.mask,
            flags
        )

        stellar_params = self.__get_stellar_params(context)
        stellar_params_covar = context.state.tempfit_results.cov
        velocity_corrections = self.__get_velocity_corrections(context.state.coadd_results.merged_spectrum.observations)
        abundances = self.__get_abundances(context)
        abundances_covar = None
        measurement_flags = self.__get_measurement_flags(context)        
        notes = PfsStarNotes()

        # TODO: where to store the global flags like tempfit_flags?
        #       these are available in tempfit_results.flags

        context.state.pfsStar = PfsStar(
            merged_spectrum.target,
            merged_spectrum.observations,
            merged_spectrum.wave,
            merged_spectrum.flux,
            merged_spectrum.mask,
            merged_spectrum.sky,
            merged_spectrum.covar,
            merged_spectrum.covar2,
            flags,
            metadata,
            flux_table,
            stellar_params,
            velocity_corrections,
            abundances,
            stellar_params_covar,
            abundances_covar,
            measurement_flags,
            notes)

        # Save output FITS file
        identity, filename = context.pipeline.save_output_product(context.state.pfsStar)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __get_stellar_params(self, context, include_snr=True):
        # Extract stellar parameters from tempfit results

        # Collect parameters
        units = {
            'T_eff': 'K',
            'log_g': 'dex',
            'M_H': 'dex',
            'a_M': 'dex',
            'v_los': 'km s-1',
        }

        # TODO: what if RV is not fitted?

        params_fit = context.state.tempfit_results.params_free + [ 'v_los' ]
        param_idx = context.state.tempfit_results.cov_params
        params_all = [ p for p in context.state.tempfit_results.params_fit ] + [ 'v_los' ]
        flags_all = {** { p: v for p, v in context.state.tempfit_results.params_flags.items() }, **{ 'v_los': context.state.tempfit_results.rv_flags }}

        # Construct columns
        method = []
        frame = []
        param = []
        covarId = []
        unit = []
        value = []
        value_err = []
        flag = []
        status = []

        for p in params_all:
            method.append('tempfit')
            frame.append('helio')
            param.append(p)
            covarId.append(param_idx.index(p) if p in param_idx else 255)
            unit.append(units[p] if p in units else '')

            # Parameter values

            if p in context.state.tempfit_results.params_fit:
                v = context.state.tempfit_results.params_fit[p]
                v_err = context.state.tempfit_results.params_err[p]
            elif p == 'v_los':
                v = context.state.tempfit_results.rv_fit
                v_err = context.state.tempfit_results.rv_err
            else:
                raise NotImplementedError()
            
            value.append(v)
            value_err.append(v_err)

            # Flags

            if p in flags_all:
                f = flags_all[p] != TempFitFlag.OK
                s = ' '.join([ m.name for m in TempFitFlag if (m.value & flags_all[p]) != 0 ])
            else:
                f = False
                s = ''
            
            flag.append(f)
            status.append(s)

        if include_snr:
            for arm in context.state.coadd_results.coadd_spectra:
                spec = context.state.coadd_results.coadd_spectra[arm][0]

                method.append('gapipe')
                frame.append('')
                param.append(f'snr_{arm}')
                covarId.append(255)
                unit.append('')

                value.append(spec.snr)
                value_err.append(0.0)

                flag.append(False)
                status.append('')

        return StellarParams(
            method=np.array(method),
            frame=np.array(frame),
            param=np.array(param),
            covarId=np.array(covarId),
            unit=np.array(unit),
            value=np.array(value),
            valueErr=np.array(value_err),
            flag=np.array(flag),
            status=np.array(status),
        )
    
    def __get_velocity_corrections(self, observations):
        # Assume observations are sorted by visit

        # TODO: not obs time data in any of the headers!
        JD = [ 0.0 for v in observations.visit]
        helio = [ 0.0 for v in observations.visit]
        bary = [ 0.0 for v in observations.visit]

        return VelocityCorrections(
            visit=np.atleast_1d(observations.visit),
            JD=np.atleast_1d(JD),
            helio=np.atleast_1d(helio),
            bary=np.atleast_1d(bary),
        )
    
    def __get_abundances(self, context):
        # TODO: implement this
        return Abundances(
            method = np.array([], dtype=str),
            element = np.array([], dtype=str),
            covarId = np.array([], dtype=np.int8),
            value = np.array([], dtype=np.float32),
            valueErr = np.array([], dtype=np.float32),
            flag = np.array([], dtype=bool),
            status = np.array([], dtype=str),
        )

    def __get_measurement_flags(self, context):
        # Measurement flags for each algorithm
        method = []
        flag = []
        status = []

        if context.config.run_tempfit:
            tempfit_flags = context.state.tempfit_results.flags
            method.append('tempfit')
            flag.append(tempfit_flags != TempFitFlag.OK)
            status.append(' '.join([ m.name for m in TempFitFlag if (m.value & tempfit_flags) != 0 ]))

        # TODO: add abundance flags

        return MeasurementFlags(
            method=np.array(method, dtype=str),
            flag=np.array(flag, dtype=bool),
            status=np.array(status, dtype=str),
        )