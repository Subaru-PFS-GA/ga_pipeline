RUN="run21"
OBSDATE="2025-03"

# Science targets

GARUN[0]="dSph_draco_S0P00_${OBSDATE}_November2025"
PROPOSAL[0]="S25A-OT02"
RERUN[0]="S25A_November2025"
OBSLOGS[0]="$OBSLOGDIR/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[0]="$DATADIR/data/targeting/dSph/draco/netflow/draco_6_1_v2/dra_targets_*.feather"
ASSIGNMENTS[0]="$DATADIR/data/targeting/dSph/draco/netflow/draco_6_?_v2/dra_assignments_all.feather"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep SSP_GA_dra_S0P00 | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]="0x0200000000-0x02FFFFFFFF"    # SSP_GA_dra

# Flux standards of the same visits

GARUN[1]="${GARUN[0]}"
PROPOSAL[1]=${PROPOSAL[0]}
RERUN[1]=${RERUN[0]}
OBSLOGS[1]=${OBSLOGS[0]}
TARGETLISTS[1]=${TARGETLISTS[0]}
ASSIGNMENTS[1]=${ASSIGNMENTS[0]}
VISITS[1]=${VISITS[0]}
CATID[1]="3006"                         # Flux standards
OBJID[1]=""

# Science targets

GARUN[2]="dSph_draco_S0P01_${OBSDATE}_November2025"
PROPOSAL[2]="S25A-OT02"
RERUN[2]="S25A_November2025"
OBSLOGS[2]="$OBSLOGDIR/runs/${OBSDATE}/obslog/*.csv"
TARGETLISTS[2]="$DATADIR/data/targeting/dSph/draco/netflow/draco_6_1_v2/dra_targets_*.feather"
ASSIGNMENTS[2]="$DATADIR/data/targeting/dSph/draco/netflow/draco_6_?_v2/dra_assignments_all.feather"
VISITS[2]="$(cat ${OBSLOGS[2]} | grep SSP_GA_dra_S0P01 | cut -d ',' -f 1)"
CATID[2]="10092"                        # GA science
OBJID[2]="0x0200000000-0x02FFFFFFFF"    # SSP_GA_dra

# Flux standards of the same visits

GARUN[3]="${GARUN[2]}"
PROPOSAL[3]=${PROPOSAL[2]}
RERUN[3]=${RERUN[2]}
OBSLOGS[3]=${OBSLOGS[2]}
TARGETLISTS[3]=${TARGETLISTS[2]}
ASSIGNMENTS[3]=${ASSIGNMENTS[2]}
VISITS[3]=${VISITS[2]}
CATID[3]="3006"                         # Flux standards
OBJID[3]=""