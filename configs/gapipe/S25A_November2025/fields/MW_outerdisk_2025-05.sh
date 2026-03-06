RUN="run22"
OBSDATE="2025-05"

GARUN="MW_outerdisk_${OBSDATE}_November2025"             # GA pipeline run name

# Science targets

#    2025-05     run22  0x1000000000             MW Outer disk l=90 b=28
#                       0x2000000000             MW Outer disk l=90 b=29

PROPOSAL[0]="S25A-OT02"
RERUN[0]="S25A_November2025"
OBSLOGS[0]="$OBSLOGDIR/runs/${OBSDATE}/obslog/*.csv"
ASSIGNMENTS[0]="$DATADIR/data/targeting/MW/outerdisk_l90_b28_SSP/netflow/outerdisk_l90_b28_6_SSP_006/outerdisk_l90_b28_faint_assignments_all.feather $DATADIR/data/targeting/MW/outerdisk_l90_b29_SSP/netflow/outerdisk_l90_b29_6_SSP_006/outerdisk_l90_b29_faint_assignments_all.feather"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep SSP_GA_outerdisk | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]="0x1000000000-0x10FFFFFFFF 0x2000000000-0x20FFFFFFFF"

# Flux standards of the same visits

PROPOSAL[1]=${PROPOSAL[0]}
RERUN[1]=${RERUN[0]}
OBSLOGS[1]=${OBSLOGS[0]}
ASSIGNMENTS[1]=${ASSIGNMENTS[0]}
VISITS[1]=${VISITS[0]}
CATID[1]="3006"                         # Flux standards
OBJID[1]=""
