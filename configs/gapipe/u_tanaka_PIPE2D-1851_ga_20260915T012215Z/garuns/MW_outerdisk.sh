# <proposal> <obs_date>         <id_prefix>              <target>
# S25A-OT02  2025-05     run22  0x1000000000             MW Outer disk l=90 b=28
#                               0x2000000000             MW Outer disk l=90 b=29
#                                                        ** cross-calibration fields
#            2025-06     run23  0x7000000000             MW Outer disk l=90 b=16
#                               0x3000000000             MW Outer disk l=90 b=-28

################################
# SSP_GA_outerdisk_l90_b28_faint

OBSDATE="2025-05"
FIELDPREFIX="MW_outerdisk_l90_b28"
OBSPREFIX="SSP_GA_outerdisk_l90_b28_faint"

# Science targets

PROPOSAL[0]="S25A-OT02"
RUN[0]="${GAPIPE_RUN}"
RUNDIR[0]="${GAPIPE_RUNDIR}"
CONFIGRUN[0]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[0]="${GAPIPE_CONFIGRUNDIR}"
GARUN[0]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[0]="${GARUN[0]}"
OBSLOGS[0]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[0]="${GAPIPE_TARGETINGDIR}/MW/outerdisk_l90_b28_SSP/netflow/outerdisk_l90_b28_6_SSP_006/outerdisk_l90_b28_faint_targets_*.feather"
ASSIGNMENTS[0]="${GAPIPE_TARGETINGDIR}/MW/outerdisk_l90_b28_SSP/netflow/outerdisk_l90_b28_6_SSP_006/outerdisk_l90_b28_faint_assignments_all.feather"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep ${OBSPREFIX} | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]="0x1000000000-0x10FFFFFFFF"

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

################################
# SSP_GA_outerdisk_l90_b29_faint

OBSDATE="2025-05"
FIELDPREFIX="MW_outerdisk_l90_b29"
OBSPREFIX="SSP_GA_outerdisk_l90_b29_faint"

# Science targets

PROPOSAL[2]="S25A-OT02"
RUN[2]="${GAPIPE_RUN}"
RUNDIR[2]="${GAPIPE_RUNDIR}"
CONFIGRUN[2]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[2]="${GAPIPE_CONFIGRUNDIR}"
GARUN[2]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[2]="${GARUN[2]}"
OBSLOGS[2]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[2]="${GAPIPE_TARGETINGDIR}/MW/outerdisk_l90_b29_SSP/netflow/outerdisk_l90_b29_6_SSP_006/outerdisk_l90_b29_faint_targets_*.feather"
ASSIGNMENTS[2]="${GAPIPE_TARGETINGDIR}/MW/outerdisk_l90_b29_SSP/netflow/outerdisk_l90_b29_6_SSP_006/outerdisk_l90_b29_faint_assignments_all.feather"
VISITS[2]="$(cat ${OBSLOGS[2]} | grep ${OBSPREFIX} | cut -d ',' -f 1)"
CATID[2]="10092"                        # GA science
OBJID[2]="0x2000000000-0x20FFFFFFFF"

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

################################
# SSP_GA_outerdisk_l90_b16_faint

OBSDATE="2025-06"
FIELDPREFIX="MW_outerdisk_l90_b16"
OBSPREFIX="SSP_GA_outerdisk_l90_b16_faint"

# Science targets

PROPOSAL[4]="S25A-OT02"
RUN[4]="${GAPIPE_RUN}"
RUNDIR[4]="${GAPIPE_RUNDIR}"
CONFIGRUN[4]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[4]="${GAPIPE_CONFIGRUNDIR}"
GARUN[4]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[4]="${GARUN[4]}"
OBSLOGS[4]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[4]="${GAPIPE_TARGETINGDIR}/MW/outerdisk_l90_b16_SSP/netflow/outerdisk_l90_b16_6_SSP_002/outerdisk_l90_b16_faint_targets_*.feather"
ASSIGNMENTS[4]="${GAPIPE_TARGETINGDIR}/MW/outerdisk_l90_b16_SSP/netflow/outerdisk_l90_b16_6_SSP_002/outerdisk_l90_b16_faint_assignments_all.feather"
VISITS[4]="$(cat ${OBSLOGS[4]} | grep ${OBSPREFIX} | cut -d ',' -f 1)"
CATID[4]="10092"                        # GA science
OBJID[4]="0x7000000000-0x70FFFFFFFF"

# Flux standards of the same visits

PROPOSAL[5]=${PROPOSAL[4]}
RUN[5]=${RUN[4]}
RUNDIR[5]=${RUNDIR[4]}
CONFIGRUN[5]="${CONFIGRUN[4]}"
CONFIGRUNDIR[5]="${CONFIGRUNDIR[4]}"
GARUN[5]="${GARUN[4]}"
GARUNDIR[5]="${GARUNDIR[4]}"
OBSLOGS[5]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[5]="${TARGETLISTS[4]}"
ASSIGNMENTS[5]=${ASSIGNMENTS[4]}
VISITS[5]=${VISITS[4]}
CATID[5]="3006"                         # Flux standards
OBJID[5]=""

################################
# SSP_GA_outerdisk_l90_bm28_faint

OBSDATE="2025-06"
FIELDPREFIX="MW_outerdisk_l90_bm28"
OBSPREFIX="SSP_GA_outerdisk_l90_bm28_faint"

# Science targets

PROPOSAL[6]="S25A-OT02"
RUN[6]="${GAPIPE_RUN}"
RUNDIR[6]="${GAPIPE_RUNDIR}"
CONFIGRUN[6]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[6]="${GAPIPE_CONFIGRUNDIR}"
GARUN[6]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[6]="${GARUN[6]}"
OBSLOGS[6]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[6]="${GAPIPE_TARGETINGDIR}/MW/outerdisk_l90_bm28_SSP/netflow/outerdisk_l90_bm28_6_SSP_006/outerdisk_l90_bm28_faint_targets_*.feather"
ASSIGNMENTS[6]="${GAPIPE_TARGETINGDIR}/MW/outerdisk_l90_bm28_SSP/netflow/outerdisk_l90_bm28_6_SSP_006/outerdisk_l90_bm28_faint_assignments_all.feather"
VISITS[6]="$(cat ${OBSLOGS[6]} | grep ${OBSPREFIX} | cut -d ',' -f 1)"
CATID[6]="10092"                        # GA science
OBJID[6]="0x3000000000-0x30FFFFFFFF"

# Flux standards of the same visits

PROPOSAL[7]=${PROPOSAL[6]}
RUN[7]=${RUN[6]}
RUNDIR[7]=${RUNDIR[6]}
CONFIGRUN[7]="${CONFIGRUN[6]}"
CONFIGRUNDIR[7]="${CONFIGRUNDIR[6]}"
GARUN[7]="${GARUN[6]}"
GARUNDIR[7]="${GARUNDIR[6]}"
OBSLOGS[7]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[7]="${TARGETLISTS[6]}"
ASSIGNMENTS[7]=${ASSIGNMENTS[6]}
VISITS[7]=${VISITS[6]}
CATID[7]="3006"                         # Flux standards
OBJID[7]=""
