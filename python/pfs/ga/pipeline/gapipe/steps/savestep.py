import os

import pfs.datamodel
from pfs.datamodel import *

from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

from ...setup_logger import logger

class SaveStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    def run(self, context):
        # Construct the output object based on the results from the pipeline steps
        # TODO: 
        metadata = {}
        flux_table = None

        stellar_params = self.__get_stellar_params(context)
        stellar_params_covar = context.pipeline.rvfit_results.cov
        velocity_corrections = self.__get_velocity_corrections(context.pipeline.coadd_results.spectrum.observations)
        abundances = self.__get_abundances(context)
        abundances_covar = None
        notes = PfsGAObjectNotes()

        # TODO: where to store the global flags like tempfit_flags?

        context.pipeline.pfsGAObject = PfsGAObject(
            context.pipeline.coadd_results.spectrum.target,
            context.pipeline.coadd_results.spectrum.observations,
            context.pipeline.coadd_results.spectrum.wave,
            context.pipeline.coadd_results.spectrum.flux,
            context.pipeline.coadd_results.spectrum.mask,
            context.pipeline.coadd_results.spectrum.sky,
            context.pipeline.coadd_results.spectrum.covar,
            context.pipeline.coadd_results.spectrum.covar2,
            MaskHelper(**{ v: k for k, v in context.pipeline.coadd_results.spectrum.mask_flags.items() }),
            metadata,
            flux_table,
            stellar_params,
            velocity_corrections,
            abundances,
            stellar_params_covar,
            abundances_covar,
            notes)

        # Save output FITS file
        identity, filename = context.pipeline.save_output_product(context.pipeline.pfsGAObject)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __get_stellar_params(self, context):
        # Extract stellar parameters from rvfit results

        # Collect parameters
        units = {
            'T_eff': 'K',
            'log_g': 'dex',
            'M_H': 'dex',
            'a_M': 'dex',
            'v_los': 'km s-1',
        }

        # TODO: what if RV is not fitted?

        params_fit = context.pipeline.rvfit_results.params_free + [ 'v_los' ]
        params_all = [ p for p in context.pipeline.rvfit_results.params_fit ] + [ 'v_los' ]
        flags_all = {** { p: v for p, v in context.pipeline.rvfit_results.params_flags.items() }, **{ 'v_los': context.pipeline.rvfit_results.rv_flags }}

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
            method.append('ga1dpipe')
            frame.append('bary')
            param.append(p)
            covarId.append(params_fit.index(p) if p in params_fit else 255)
            unit.append(units[p] if p in units else '')

            # Parameter values

            if p in context.pipeline.rvfit_results.params_fit:
                v = context.pipeline.rvfit_results.params_fit[p]
                v_err = context.pipeline.rvfit_results.params_err[p]
            elif p == 'v_los':
                v = context.pipeline.rvfit_results.rv_fit
                v_err = context.pipeline.rvfit_results.rv_err
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