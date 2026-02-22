RUN="run21"
OBSDATE="2025-03"

GARUN="dSph_draco_${OBSDATE}_November2025"             # GA pipeline run name

# Direct targets

PROPOSAL[0]="S25A-OT02"
RERUN[0]="S25A_November2025"
OBSLOGS[0]="$OBSLOGDIR/runs/${OBSDATE}/obslog/*.csv"
ASSIGNMENTS[0]="$DATADIR/data/targeting/dSph/draco/netflow/draco_6_0_v2/dra_assignments_all.feather"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep SSP_GA_dra | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]="0x0200000000-0x02FFFFFFFF"    # SSP_GA_dra

# Flux standards of the same visits

PROPOSAL[1]=${PROPOSAL[0]}
RERUN[1]=${RERUN[0]}
OBSLOGS[1]=${OBSLOGS[0]}
ASSIGNMENTS[1]=${ASSIGNMENTS[0]}
VISITS[1]=${VISITS[0]}
CATID[1]="3006"                         # Flux standards
OBJID[1]=""
