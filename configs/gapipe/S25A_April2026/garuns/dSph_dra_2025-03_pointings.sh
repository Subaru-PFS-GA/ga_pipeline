OBSDATE="2025-03"
FIELDPREFIX="dSph_dra"

# P00

OBSPREFIX="SSP_GA_dra_S0P00"

# Science targets

PROPOSAL[0]="S25A-OT02"
RUN[0]="${GAPIPE_RUN}"
RUNDIR[0]="${GAPIPE_RUNDIR}"
CONFIGRUN[0]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[0]="${GAPIPE_CONFIGRUNDIR}"
GARUN[0]="${FIELDPREFIX}_S0P00_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
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

# P01

OBSPREFIX="SSP_GA_dra_S0P01"

# Science targets

PROPOSAL[2]="S25A-OT02"
RUN[2]="${GAPIPE_RUN}"
RUNDIR[2]="${GAPIPE_RUNDIR}"
CONFIGRUN[2]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[2]="${GAPIPE_CONFIGRUNDIR}"
GARUN[2]="${FIELDPREFIX}_S0P01_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[2]="${GARUN[2]}"
OBSLOGS[2]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[2]="${GAPIPE_TARGETINGDIR}/dSph/draco/netflow/draco_6_1_v2/dra_targets_*.feather"
ASSIGNMENTS[2]="${GAPIPE_TARGETINGDIR}/dSph/draco/netflow/draco_6_?_v2/dra_assignments_all.feather"
VISITS[2]="$(cat ${OBSLOGS[2]} | grep $OBSPREFIX | cut -d ',' -f 1)"
CATID[2]="10092"                        # GA science
OBJID[2]="0x0200000000-0x02FFFFFFFF"    # Draco

# Flux standards of the same visits

PROPOSAL[3]=${PROPOSAL[2]}
RUN[3]=${RUN[2]}
RUNDIR[3]=${RUNDIR[2]}
CONFIGRUN[3]="${CONFIGRUN[2]}"
CONFIGRUNDIR[3]="${CONFIGRUNDIR[2]}"
GARUN[3]="${GARUN[2]}"
GARUNDIR[3]="${GARUNDIR[2]}"
OBSLOGS[3]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[3]="${TARGETLISTS[2]}"
ASSIGNMENTS[3]=${ASSIGNMENTS[2]}
VISITS[3]=${VISITS[2]}
CATID[3]="3006"                         # Flux standards
OBJID[3]=""