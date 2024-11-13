import os

import pfs.datamodel
from pfs.datamodel import *

from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

from ...setup_logger import logger

class VCorrStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

        self.__v_corr = None                    # velocity correction for each visit

    def run(self, context):
        # if self.config.v_corr is not None and self.config.v_corr.lower() != 'none':
        #     self.__v_corr_calculate()
        #     self.__v_corr_apply()
        #     return StepResults(success=True, skip_remaining=False, skip_substeps=False)
        # else:
        #     logger.info('Velocity correction for geocentric frame is set to `none`, skipping corrections.')
        #     return StepResults(success=True, skip_remaining=False, skip_substeps=True)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=True)
    
    def __v_corr_calculate(self):
        
        # TODO: logging + perf counter

        # TODO: review this
        raise NotImplementedError()
        
        self.__v_corr = {}

        # Calculate the velocity correction for each spectrum
        for arm in self.__spectra:
            for visit in self.__spectra[arm]:
                s = self.__spectra[arm][visit]
                if s is not None and visit not in self.__v_corr:
                    self.__v_corr[visit] = Astro.v_corr(self.config.v_corr, s.ra, s.dec, s.mjd)

    def __v_corr_apply(self):

        # TODO: review this
        raise NotImplementedError()

        # Apply the velocity correction for each spectrum
        for arm in self.__spectra:
            for visit in self.__spectra[arm]:
                s = self.__spectra[arm][visit]

                if s is not None:
                    # Apply the correction to the spectrum
                    # TODO: verify this and convert into a function on spectrum
                    z = Physics.vel_to_z(self.__v_corr[visit])
                    s.apply_v_corr(z=z)