import numpy as np

from pfs.ga.pfsspec.core.plotting import SpectrumPlot, styles
from pfs.ga.pfsspec.core.util.args import *
from pfs.ga.pfsspec.core import Trace, SpectrumTrace

from ..common import PipelineTrace

class GAPipelineTrace(PipelineTrace, SpectrumTrace):
    def __init__(self,
                 figdir='.',
                 logdir='.',
                 plot_inline=False, 
                 plot_level=Trace.PLOT_LEVEL_INFO, 
                 log_level=Trace.LOG_LEVEL_INFO,
                 id=None):
        
        self.__id = id                          # Identity represented as string
        
        self.plot_exposures = None
        self.plot_coadd_flux_correction = None
        self.plot_coadd_input = None
        self.plot_coadd_stack = None
        self.plot_coadd_merged = None

        super().__init__(figdir=figdir, logdir=logdir,
                         plot_inline=plot_inline, 
                         plot_level=plot_level,
                         log_level=log_level)

    def reset(self):
        super().reset()

    def update(self, figdir=None, logdir=None, id=None):
        super().update(figdir=figdir, logdir=logdir)
        
        self.__id = id if id is not None else self.__id

    #region Properties

    def __get_id(self):
        return self.__id
    
    def __set_id(self, value):
        self.__id = value

    id = property(__get_id, __set_id)

    #endregion

    def add_args(self, parser):
        super().add_args(parser)
    
    def init_from_args(self, script, args):
        SpectrumTrace.init_from_args(self, script, None, args)

        self.plot_exposures = get_arg('plot_exposures', self.plot_exposures, args)
        self.plot_coadd_flux_correction = get_arg('plot_coadd_flux_correction', self.plot_coadd_flux_correction, args)
        self.plot_coadd_input = get_arg('plot_coadd_input', self.plot_coadd_input, args)
        self.plot_coadd_stack = get_arg('plot_coadd_stack', self.plot_coadd_stack, args)
        self.plot_coadd_merged = get_arg('plot_coadd_merged', self.plot_coadd_merged, args)

    def on_load(self, spectra):
        """Fired when the individual exposures are read from the pfsSingle files."""

        if self.plot_exposures is None and self.plot_level >= Trace.PLOT_LEVEL_DEBUG \
            or self.plot_exposures:

            self._plot_spectra('pfsGA-exposures-{id}',
                               spectra,
                               plot_flux=True, plot_flux_err=True, plot_mask=True,
                               print_snr=True,
                               title='Input spectra - {id}',
                               nrows=2, ncols=1, diagram_size=(6.5, 3.5))
            
            self.flush_figures()

    def on_coadd_get_templates(self, spectra, templates):
        """Fired when the templates at the best fit parameters are loaded."""
        
        pass

    def on_coadd_eval_correction(self, spectra, templates, corrections, correction_masks, spec_norm, temp_norm):
        """Fired when the flux correction is evaluated."""

        if self.plot_coadd_flux_correction is None and self.plot_level >= Trace.PLOT_LEVEL_DEBUG \
            or self.plot_coadd_flux_correction:

            style = styles.thin_line()
            style['alpha'] = 0.25

            self._plot_spectra('pfsGA-coadd-corr-{id}',
                               spectra,
                               single_plot=True,
                               plot_flux=True, plot_flux_err=False, plot_mask=False,
                               normalize_cont=True, apply_flux_corr=True,
                               title='Coadd corrected input spectra - {id}',
                               nrows=2, ncols=1, diagram_size=(6.5, 3.5),
                               **style)

            # TODO: update this to allow for continumm fitting and flux correction

            self.flush_figures()

    def on_coadd_start_stack(self, spectra, no_continuum_bit, exclude_bits):
        """
        Fired when the coaddition process starts.
        
        Parameters
        ----------
        spectra : dict
            The dictionary of spectra, the continuum model or the flux correction attached
            but not applied yet.
        no_continuum_bit : int
            The bit mask for the no continuum flag.
        exclude_bits : list
            The list of bit masks to exclude from coadding.
        """

        if self.plot_coadd_input is None and self.plot_level >= Trace.PLOT_LEVEL_DEBUG \
            or self.plot_coadd_input:

            self._plot_spectra('pfsGA-coadd-input-{id}',
                               spectra,
                               plot_flux=True, plot_flux_err=True, plot_mask=True,
                               mask_bits=no_continuum_bit | exclude_bits,
                               normalize_cont=True, apply_flux_corr=True,
                               title='Coadd input spectra - {id}',
                               nrows=2, ncols=1, diagram_size=(6.5, 3.5))

            self.flush_figures()

    def on_coadd_finish_stack(self, coadd_spectra, arms, no_continuum_bit, exclude_bits):
        """
        Fired when the coaddition process finishes.
        
        Parameters
        ----------
        spectra : dict
            The dictionary of spectra, the continuum model or the flux correction attached
            but not applied yet.
        no_continuum_bit : int
            The bit mask for the no continuum flag.
        exclude_bits : list
            The list of bit masks to exclude from coadding.
        """

        if self.plot_coadd_input is None and self.plot_level >= Trace.PLOT_LEVEL_DEBUG \
            or self.plot_coadd_input:

            # TODO: plot each exposure that goes into coadd
            pass


        if self.plot_coadd_stack is None and self.plot_level >= Trace.PLOT_LEVEL_INFO \
            or self.plot_coadd_stack:

            self._plot_spectra('pfsGA-coadd-stack-{id}',
                                coadd_spectra,
                                plot_flux=True, plot_flux_err=True, plot_mask=True,
                                mask_bits=no_continuum_bit | exclude_bits,
                                normalize_cont=False, apply_flux_corr=False,
                                title='Coadd stacked spectrum - {id}',
                                diagram_size=(6.5, 3.5))

            self.flush_figures()

    def on_coadd_finish_merged(self, merged_spectrum, arms):
        """
        Fired when the coaddition process finishes and the merged spectrum is created.
        
        Parameters
        ----------
        merged_spectrum : PfsSpectrum
            The merged spectrum.
        """

        if self.plot_coadd_merged is None and self.plot_level >= Trace.PLOT_LEVEL_INFO \
            or self.plot_coadd_merged:

            self._plot_spectrum('pfsGA-coadd-merged-{id}',
                                arm=arms,
                                spectrum=merged_spectrum,
                                plot_flux=True, plot_flux_err=True, plot_mask=True,
                                normalize_cont=False, apply_flux_corr=False,
                                title='Coadd merged spectrum - {id}',
                                diagram_size=(6.5, 3.5))

            self.flush_figures()