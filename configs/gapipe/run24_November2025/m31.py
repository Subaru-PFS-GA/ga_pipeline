import os
from pfs.ga.pfsspec.core import Trace

config = dict(
    tempfit = dict(
        tempfit_args = dict(
            M_H = [ -5.0, 0.5 ],
            M_H_dist = [ "normal", -1.0, 2.0 ],
            
            # Roman's grid
            # a_M = 0.0,
            # C = 0.0,
            
            a_M = [ -1.2, 0.8 ],
            a_M_dist = [ "normal", 0.0, 0.5 ],
            C = [ -0.2, 0.2 ],
        ),
    ),
)
