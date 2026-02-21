RUN="run22"
OBSDATE="2025-05"

GARUN="MW_outerdisk_${OBSDATE}_November2025"             # GA pipeline run name

# Direct targets

PROPOSAL[0]="S25A-OT02"
RERUN[0]="S25A_November2025"
OBSLOGS[0]="$OBSLOGDIR/runs/${OBSDATE}/obslog/*.csv"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep SSP_GA_outerdisk | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]="0x1000000000-0x10FFFFFFFF 0x2000000000-0x20FFFFFFFF"

# Flux standards of the same visits

PROPOSAL[1]=${PROPOSAL[0]}
RERUN[1]=${RERUN[0]}
OBSLOGS[1]=${OBSLOGS[0]}
VISITS[1]=${VISITS[0]}
CATID[1]="3006"                         # Flux standards
OBJID[1]=""
