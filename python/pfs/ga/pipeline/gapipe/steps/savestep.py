import os
import numpy as np

import pfs.datamodel
from pfs.datamodel import *
from pfs.ga.pfsspec.survey.pfs.utils import *

from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

from ...setup_logger import logger

class SaveStep(PipelineStep):

    UNITS = {
        'T_eff': 'K',
        'log_g': '',
        'M_H': 'dex',
        'a_M': 'dex',
        'v_los': 'km s-1',
        'E(B-V)': 'mag',
    }

    def __init__(self, name=None):
        super().__init__(name)

    def run(self, context):

        # Construct the output object based on the results from the pipeline steps

        if not context.config.run_tempfit:
            raise NotImplementedError("Tempfit step must be run before SaveStep.")

        # Collect all observations used for fitting
        target = None
        observations = []

        for arm in context.state.tempfit_spectra:
            for s in context.state.tempfit_spectra[arm]:
                if s is not None:
                    observations.append(s.observations)
                    if target is None:
                        target = s.target

        observations = merge_observations(observations)
        velocity_corrections = self.__get_velocity_corrections(observations)
        measurement_flags = self.__get_measurement_flags(context)
         
        # Collect fit results
        stellar_params_tempfit = self.__get_stellar_params_tempfit(context)
        stellar_params_covar = context.state.tempfit_results.cov

        if context.config.run_chemfit:
            stellar_params_chemfit = self.__get_stellar_params_chemfit(context)
            stellar_params = self.__combine_stellar_params(stellar_params_tempfit, stellar_params_chemfit)

            abundances = self.__get_abundances_chemfit(context)
            abundances_covar = context.state.chemfit_results.cov
        else:
            stellar_params = stellar_params_tempfit

            abundances = None
            abundances_covar = None
        
        notes = PfsStarNotes()
        metadata = {}

        # We can store the spectrum only if the coadd step was run
        # If chemfit was also run, it should take precedence over the model
        # computed for the coadd spectrum
        if context.config.run_chemfit:
            merged_spectrum = context.state.chemfit_results.merged_spectrum

            flags = MaskHelper(**{ v: k for k, v in merged_spectrum.mask_flags.items() })

            # Construct the flux table, this is an alternative representation of the spectrum
            shape = merged_spectrum.wave.shape

            wave, _ = merged_spectrum.wave_in_unit('nm')
            flux, flux_err = merged_spectrum.flux_in_unit('nJy')
            sky = merged_spectrum.sky_in_unit('nJy')
            mask = merged_spectrum.mask
            if merged_spectrum.line_model is not None:
                cont = merged_spectrum.cont_in_unit('nJy') if merged_spectrum.cont is not None else np.zeros(shape)
                flux_model = cont * merged_spectrum.line_model
                norm_flux = flux / cont if cont is not None else np.zeros(shape)
                norm_err = merged_spectrum.flux_err / merged_spectrum.cont if merged_spectrum.cont is not None else np.zeros(shape)
                norm_model = merged_spectrum.line_model
            else:
                raise NotImplementedError()
                norm_flux = merged_spectrum.flux / merged_spectrum.cont if merged_spectrum.cont is not None else np.zeros(shape)
                norm_err = merged_spectrum.flux_err / merged_spectrum.cont if merged_spectrum.cont is not None else np.zeros(shape)
                norm_model = merged_spectrum.flux_model / merged_spectrum.cont if (merged_spectrum.flux_model is not None and merged_spectrum.cont is not None) else np.zeros(shape)
            
            # TODO: unit conversion!
            covar = merged_spectrum.covar
            covar2 = merged_spectrum.covar2
        elif context.config.run_coadd:
            merged_spectrum = context.state.coadd_results.merged_spectrum
            flags = MaskHelper(**{ v: k for k, v in merged_spectrum.mask_flags.items() })

            # Construct the flux table, this is an alternative representation of the spectrum
            shape = merged_spectrum.wave.shape

            wave, _ = merged_spectrum.wave_in_unit('nm')
            flux, flux_err = merged_spectrum.flux_in_unit('nJy')
            sky = merged_spectrum.sky_in_unit('nJy')
            mask = merged_spectrum.mask
            flux_model = merged_spectrum.model_in_unit('nJy') if merged_spectrum.flux_model is not None else np.zeros(shape)
            cont = merged_spectrum.cont_in_unit('nJy') if merged_spectrum.cont is not None else np.zeros(shape)
            norm_flux = merged_spectrum.flux / merged_spectrum.cont if merged_spectrum.cont is not None else np.zeros(shape)
            norm_err = merged_spectrum.flux_err / merged_spectrum.cont if merged_spectrum.cont is not None else np.zeros(shape)
            norm_model = merged_spectrum.flux_model / merged_spectrum.cont if (merged_spectrum.flux_model is not None and merged_spectrum.cont is not None) else np.zeros(shape)
            
            # TODO: unit conversion!
            covar = merged_spectrum.covar
            covar2 = merged_spectrum.covar2
        else:
            # Dummy data to be able to save the FITS file
            wave = np.array([0.0], dtype=float)
            flux = np.array([0.0], dtype=float)
            flux_err = np.array([0.0], dtype=float)
            sky = np.array([0.0], dtype=float)
            mask = np.array([0], dtype=np.int32)
            flux_model = np.array([0.0], dtype=float)
            cont = np.array([0.0], dtype=float)
            norm_flux = np.array([0.0], dtype=float)
            norm_err = np.array([0.0], dtype=float)
            norm_model = np.array([0.0], dtype=float)
            covar = np.array([[0.0]], dtype=float)
            covar2 = np.array([[0.0]], dtype=float)
            flags = MaskHelper()
            

        flux_table = StarFluxTable(
            wave,
            flux,
            flux_err,
            flux_model,                 # Best-fit fluxed model
            cont,                       # Model continuum
            norm_flux,                  # Continuum-normalized flux
            norm_err,                   # Error of continuum-normalized flux
            norm_model,                 # Continuum-normalized model
            mask,
            flags
        )

        # TODO: where to store the global flags like tempfit_flags?
        #       these are available in tempfit_results.flags

        context.state.pfsStar = PfsStar(
            target = target,
            observations = observations,
            wavelength = wave,
            flux = flux,
            mask = mask,
            sky = sky,
            covar = covar,
            covar2 = covar2,
            flags = flags,
            metadata = metadata,
            fluxTable = flux_table,
            stellarParams = stellar_params,
            velocityCorrections = velocity_corrections,
            abundances = abundances,
            paramsCovar = stellar_params_covar,
            abundCovar = abundances_covar,
            measurementFlags = measurement_flags,
            notes = notes
        )

        # Save output FITS file
        identity, filename = context.pipeline.save_output_product(context.state.pfsStar)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def __combine_stellar_params(self, *stellar_params_list):
        method = []
        frame = []
        param = []
        covarId = []
        unit = []
        value = []
        value_err = []
        flag = []
        status = []

        for params in stellar_params_list:
            method.extend(params.method)
            frame.extend(params.frame)
            param.extend(params.param)
            covarId.extend(params.covarId)
            unit.extend(params.unit)
            value.extend(params.value)
            value_err.extend(params.valueErr)
            flag.extend(params.flag)
            status.extend(params.status)

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
    
    def __get_stellar_params_tempfit(self, context, include_snr=True):
        # Extract stellar parameters from tempfit results

        # TODO: add carbon

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
            unit.append(self.UNITS[p] if p in self.UNITS else '')

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

        if include_snr and context.config.run_coadd:
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

    def __get_stellar_params_chemfit(self, context, include_snr=True):

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

        for p in context.state.chemfit_results.params_fit:
            method.append('chemfit')
            frame.append('rest')
            param.append(p)

            if p in context.state.chemfit_results.cov_params:
                covarId.append(context.state.chemfit_results.cov_params.index(p))
            else:
                covarId.append(255)

            unit.append(self.UNITS[p] if p in self.UNITS else '')

            value.append(context.state.chemfit_results.params_fit[p])
            value_err.append(context.state.chemfit_results.params_err[p])
            flag.append(context.state.chemfit_results.params_flags[p] != ChemFitFlag.OK)
            status.append(' '.join([
                m.name for m in ChemFitFlag
                if (m.value & context.state.chemfit_results.params_flags[p]) != 0
            ]))

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

    def __get_abundances_chemfit(self, context):

        method = []
        element = []
        covarId = []
        value = []
        value_err = []
        flag = []
        status = []

        for p in context.state.chemfit_results.abund_fit:
            method.append('chemfit')
            element.append(p)

            if p in context.state.chemfit_results.cov_params:
                covarId.append(context.state.chemfit_results.cov_params.index(p))
            else:
                covarId.append(255)

            value.append(context.state.chemfit_results.abund_fit[p])
            value_err.append(context.state.chemfit_results.abund_err[p])
            flag.append(context.state.chemfit_results.abund_flags[p] != ChemFitFlag.OK)
            status.append(' '.join([
                m.name for m in ChemFitFlag
                if (m.value & context.state.chemfit_results.abund_flags[p]) != 0
            ]))

        return Abundances(
            method=np.array(method),
            element=np.array(element),
            covarId=np.array(covarId),
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
        if context.config.run_chemfit:
            chemfit_flags = context.state.chemfit_results.flags
            method.append('chemfit')
            flag.append(chemfit_flags != ChemFitFlag.OK)
            status.append(' '.join([ m.name for m in ChemFitFlag if (m.value & chemfit_flags) != 0 ]))

        return MeasurementFlags(
            method=np.array(method, dtype=str),
            flag=np.array(flag, dtype=bool),
            status=np.array(status, dtype=str),
        )