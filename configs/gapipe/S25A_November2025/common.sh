# Data locations
export GAPIPE_DATADIR="${GAPIPE_DATAROOT}/S25A-OT02/2d"
export GAPIPE_RUN="S25A_November2025"
export GAPIPE_RUNDIR="S25A_November2025"
export GAPIPE_CONFIGRUN="S25A_November2025"
export GAPIPE_CONFIGRUNDIR="S25A_November2025"

# Pipe2d processing run -- when using butler
# export GAPIPE_USE_BUTLER=0
export GAPIPE_USE_BUTLER=1
export BUTLER_CONFIGDIR="${GAPIPE_DATADIR}"
export BUTLER_COLLECTIONS="S25A_November2025"

OBSLOGS="${GAPIPE_OBSLOGDIR}/runs/2025-03/obslog/*.csv ${GAPIPE_OBSLOGDIR}/runs/2025-06/obslog/*.csv"
OBSPREFIX="SSP_GA_"
export GAPIPE_ALLVISITS=($(cat ${OBSLOGS} | grep "${OBSPREFIX}" | cut -d ',' -f 1))