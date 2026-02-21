RUN="run24"
OBSDATE="2025-09"

GARUN="M31_${OBSDATE}_November2025"             # GA pipeline run name

# Direct targets

PROPOSAL[0]="S25B-OT02"
RERUN[0]="run24_November2025"
OBSLOGS[0]="$OBSLOGDIR/runs/${OBSDATE}/obslog/*.csv"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep SSP_GA_m31 | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]="0x100000100000000-0x10000FFFFFFFFFF"

# Flux standards of the same visits

PROPOSAL[1]=${PROPOSAL[0]}
RERUN[1]=${RERUN[0]}
OBSLOGS[1]=${OBSLOGS[0]}
VISITS[1]=${VISITS[0]}
CATID[1]="3006"                         # Flux standards
OBJID[1]=""
