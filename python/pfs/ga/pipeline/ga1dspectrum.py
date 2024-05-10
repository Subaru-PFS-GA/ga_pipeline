from pfs.ga.pfsspec.core import Spectrum
from pfs.ga.pfsspec.stellar import StellarSpectrum

class GA1DSpectrum(StellarSpectrum, Spectrum):
    # Note: this class would normally be part of pfs.ga.pfsspec.survey but we
    #       don't want to reference that project here

    def __init__(self, orig=None):
        Spectrum.__init__(self, orig=orig)
        StellarSpectrum.__init__(self, orig=orig)

        if not isinstance(orig, GA1DSpectrum):
            self.catId = None
            self.objId = None
            self.visit = None

            self.spectrograph = None
            self.fiberId = None

            self.ra = None
            self.dec = None
            self.mjd = None

            self.airmass = None
        else:
            self.catId = orig.catId
            self.objId = orig.objId
            self.visit = orig.visit

            self.spectrograph = orig.spectrograph
            self.fiberId = orig.fiberId

            self.ra = orig.ra
            self.dec = orig.dec
            self.mjd = orig.mjd

            self.airmass = orig.airmass
            
    def get_param_names(self):
        params = ['objid',
                  'visit',
                  'ra',
                  'dec',
                  'mjd']
        return params

    def get_id_string(self):
        return f'catId={self.catId:05d}, objID={self.objId:016x}, visit={self.visit:06d}'