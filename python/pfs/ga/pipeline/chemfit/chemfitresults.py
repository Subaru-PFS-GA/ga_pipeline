class ChemFitResults():
    def __init__(
        self, /,
        rv_fit=None, rv_err=None, rv_mcmc=None, rv_flags=None,
        params_free=None, params_fit=None, params_err=None, params_mcmc=None, params_flags=None,
        abund_free=None, abund_fit=None, abund_err=None, abund_mcmc=None, abund_flags=None,
        accept_rate=None,
        jac=None, jac_params=None,
        cov=None, cov_params=None,
        flags=None,
        orig=None):

        if not isinstance(orig, ChemFitResults):
            self.rv_fit = rv_fit                        # Best fit RV
            self.rv_err = rv_err                        # Best fit RV uncertainty
            self.rv_mcmc = rv_mcmc                      # RV MC samples
            self.rv_flags = rv_flags                    # RV flags
            self.params_free = params_free              # List of free model parameters
            self.params_fit = params_fit                # Best fit model parameters
            self.params_err = params_err                # Best fit model parameter uncertainties
            self.params_mcmc = params_mcmc              # Model parameter MC samples
            self.params_flags = params_flags            # Model parameter flags
            self.abund_free = abund_free                # List of free abundances
            self.abund_fit = abund_fit                  # Best fit abundances
            self.abund_err = abund_err                  # Best fit abundance uncertainties
            self.abund_mcmc = abund_mcmc                # Abundance MC samples
            self.abund_flags = abund_flags              # Abundance flags
            self.accept_rate = accept_rate              # MC acceptance rate
            self.jac = jac                              # Jacobian of the model with respect to RV
            self.jac_params = jac_params                # Indexes of parameters in the Jacobian
            self.cov = cov                              # Covariance matrix
            self.cov_params = cov_params                # Indexes of parameters in the covariance matrix
            self.flags = flags
        else:
            self.rv_fit = orig.rv_fit
            self.rv_err = orig.rv_err
            self.rv_mcmc = orig.rv_mcmc
            self.rv_flags = orig.rv_flags
            self.params_free = orig.params_free
            self.params_fit = orig.params_fit
            self.params_err = orig.params_err
            self.params_mcmc = orig.params_mcmc
            self.params_flags = orig.params_flags
            self.abund_free = orig.abund_free
            self.abund_fit = orig.abund_fit
            self.abund_err = orig.abund_err
            self.abund_mcmc = orig.abund_mcmc
            self.abund_flags = orig.abund_flags
            self.accept_rate = orig.accept_rate
            self.jac = orig.jac
            self.jac_params = orig.jac_params
            self.cov = orig.cov
            self.cov_params = orig.cov_params
            self.flags = orig.flags
            