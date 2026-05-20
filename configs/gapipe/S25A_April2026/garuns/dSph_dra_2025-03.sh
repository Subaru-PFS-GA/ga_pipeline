OBSDATE="2025-03"
FIELDPREFIX="dSph_dra"
OBSPREFIX="SSP_GA_dra"

# Science targets

PROPOSAL[0]="S25A-OT02"
RUN[0]="${GAPIPE_RUN}"
RUNDIR[0]="${GAPIPE_RUNDIR}"
CONFIGRUN[0]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[0]="${GAPIPE_CONFIGRUNDIR}"
GARUN[0]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[0]="${GARUN[0]}"
OBSLOGS[0]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[0]="${GAPIPE_TARGETINGDIR}/dSph/draco/netflow/draco_6_1_v2/dra_targets_*.feather"
ASSIGNMENTS[0]="${GAPIPE_TARGETINGDIR}/dSph/draco/netflow/draco_6_?_v2/dra_assignments_all.feather"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep $OBSPREFIX | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]="0x0200000000-0x02FFFFFFFF"    # Draco

# Flux standards of the same visits

PROPOSAL[1]=${PROPOSAL[0]}
RUN[1]=${RUN[0]}
RUNDIR[1]=${RUNDIR[0]}
CONFIGRUN[1]="${CONFIGRUN[0]}"
CONFIGRUNDIR[1]="${CONFIGRUNDIR[0]}"
GARUN[1]="${GARUN[0]}"
GARUNDIR[1]="${GARUNDIR[0]}"
OBSLOGS[1]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[1]="${TARGETLISTS[0]}"
ASSIGNMENTS[1]=${ASSIGNMENTS[0]}
VISITS[1]=${VISITS[0]}
CATID[1]="3006"                         # Flux standards
OBJID[1]=""
