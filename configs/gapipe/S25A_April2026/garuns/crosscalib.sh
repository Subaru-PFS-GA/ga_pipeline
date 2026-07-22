OBSDATE="2025-05"
FIELDPREFIX="crosscalib"

######

OBSPREFIX="SSP_GA_crosscalib_ra288_decm11"

# Science targets

PROPOSAL[0]="S25A-OT02"
RUN[0]="${GAPIPE_RUN}"
RUNDIR[0]="${GAPIPE_RUNDIR}"
CONFIGRUN[0]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[0]="${GAPIPE_CONFIGRUNDIR}"
GARUN[0]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[0]="${GARUN[0]}"
OBSLOGS[0]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[0]="${GAPIPE_TARGETINGDIR}/CC/crosscalib_ra288_dec-11/netflow/crosscalib_ra288_dec-11_1_SSP_005/crosscalib_ra288_decm11_targets_*.feather"
ASSIGNMENTS[0]="${GAPIPE_TARGETINGDIR}/CC/crosscalib_ra288_dec-11/netflow/crosscalib_ra288_dec-11_1_SSP_005/crosscalib_ra288_decm11_assignments_all.feather"
# VISITS[0]="$(cat ${OBSLOGS[0]} | grep $OBSPREFIX | cut -d ',' -f 1)"
VISITS[0]="126587 126588 126589"
CATID[0]="10092"                        # GA science
OBJID[0]=""                             # Any

######

OBSPREFIX="SSP_GA_crosscalib_ra336_decm12"

PROPOSAL[1]="S25A-OT02"
RUN[1]="${GAPIPE_RUN}"
RUNDIR[1]="${GAPIPE_RUNDIR}"
CONFIGRUN[1]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[1]="${GAPIPE_CONFIGRUNDIR}"
GARUN[1]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[1]="${GARUN[1]}"
OBSLOGS[1]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[1]="${GAPIPE_TARGETINGDIR}/CC/crosscalib_ra336_dec-12/netflow/crosscalib_ra336_dec-12_1_SSP_004/crosscalib_ra336_decm12_targets_*.feather"
ASSIGNMENTS[1]="${GAPIPE_TARGETINGDIR}/CC/crosscalib_ra336_dec-12/netflow/crosscalib_ra336_dec-12_1_SSP_004/crosscalib_ra336_decm12_assignments_all.feather"
VISITS[1]="$(cat ${OBSLOGS[1]} | grep $OBSPREFIX | cut -d ',' -f 1)"
CATID[1]="10092"                        # GA science
OBJID[1]=""                             # Any

######

OBSPREFIX="SSP_GA_crosscalib_ra288_decm17"

PROPOSAL[2]="S25A-OT02"
RUN[2]="${GAPIPE_RUN}"
RUNDIR[2]="${GAPIPE_RUNDIR}"
CONFIGRUN[2]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[2]="${GAPIPE_CONFIGRUNDIR}"
GARUN[2]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[2]="${GARUN[2]}"
OBSLOGS[2]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[2]="${GAPIPE_TARGETINGDIR}/CC/crosscalib_ra288_dec-17/netflow/crosscalib_ra288_dec-17_1_SSP_004/crosscalib_ra288_decm17_targets_*.feather"
ASSIGNMENTS[2]="${GAPIPE_TARGETINGDIR}/CC/crosscalib_ra288_dec-17/netflow/crosscalib_ra288_dec-17_1_SSP_004/crosscalib_ra288_decm17_assignments_all.feather"
VISITS[2]="$(cat ${OBSLOGS[2]} | grep $OBSPREFIX | cut -d ',' -f 1)"
CATID[2]="10092"                        # GA science
OBJID[2]=""                             # Any

######

OBSPREFIX="SSP_GA_crosscalib_ra288_decm22"

PROPOSAL[3]="S25A-OT02"
RUN[3]="${GAPIPE_RUN}"
RUNDIR[3]="${GAPIPE_RUNDIR}"
CONFIGRUN[3]="${GAPIPE_CONFIGRUN}"
CONFIGRUNDIR[3]="${GAPIPE_CONFIGRUNDIR}"
GARUN[3]="${FIELDPREFIX}_${OBSDATE}_${GAPIPE_RUN}_${GAPIPE_CONFIG}"
GARUNDIR[3]="${GARUN[3]}"
OBSLOGS[3]="${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[3]="${GAPIPE_TARGETINGDIR}/CC/crosscalib_ra288_dec-22/netflow/crosscalib_ra288_dec-22_1_SSP_005/crosscalib_ra288_decm22_targets_*.feather"
ASSIGNMENTS[3]="${GAPIPE_TARGETINGDIR}/CC/crosscalib_ra288_dec-22/netflow/crosscalib_ra288_dec-22_1_SSP_005/crosscalib_ra288_decm22_assignments_all.feather"
VISITS[3]="$(cat ${OBSLOGS[3]} | grep $OBSPREFIX | cut -d ',' -f 1)"
CATID[3]="10092"                        # GA science
OBJID[3]=""                             # Any