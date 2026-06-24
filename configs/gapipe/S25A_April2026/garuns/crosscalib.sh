OBSDATE="2025-05"
FIELDPREFIX="crosscalib"
OBSPREFIX="SSP_GA_crosscalib"

# Science targets

PROPOSAL[0]="S25A-OT02"
RUN[0]="${GAPIPE_RUN}"
RUNDIR[0]="${GAPIPE_RUNDIR}"
CONFIGRUN[0]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[0]="${GAPIPE_CONFIGRUNDIR}"
GARUN[0]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[0]="${GARUN[0]}"
OBSLOGS[0]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
# TARGETLISTS[0]="${GAPIPE_TARGETINGDIR}/dSph/draco/netflow/draco_6_1_v2/dra_targets_*.feather"
# ASSIGNMENTS[0]="${GAPIPE_TARGETINGDIR}/dSph/draco/netflow/draco_6_?_v2/dra_assignments_all.feather"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep $OBSPREFIX | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]=""                             # Any