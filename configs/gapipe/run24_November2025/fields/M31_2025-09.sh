RUN="run24"
OBSDATE="2025-09"

GARUN="M31_${OBSDATE}_November2025"             # GA pipeline run name

# Science targets

# S25B-OT02  2025-09     run24  0x100000100000000        M31 E0
#                               0x100000200000000        M31 W0
#                               0x100000300000000        M31 GSS0
#                               0x100000400000000        M31 NWS0

data/targeting/m31/m31_E0_SSP/netflow/m31_E0_1_SSP_007/m31_assignments_all.feather

PROPOSAL[0]="S25B-OT02"
RERUN[0]="run24_November2025"
OBSLOGS[0]="$OBSLOGDIR/runs/${OBSDATE}/obslog/*.csv"
ASSIGNMENTS[0]="
    $DATADIR/data/targeting/m31/m31_E0_SSP/netflow/m31_E0_1_SSP_004/m31_assignments_all.feather
    $DATADIR/data/targeting/m31/m31_W0_SSP/netflow/m31_W0_1_SSP_004/m31_assignments_all.feather
    $DATADIR/data/targeting/m31/m31_GSS0_SSP/netflow/m31_GSS0_1_SSP_004/m31_assignments_all.feather
    $DATADIR/data/targeting/m31/m31_NWS0_SSP/netflow/m31_NWS0_1_SSP_004/m31_assignments_all.feather"
VISITS[0]="$(cat ${OBSLOGS[0]} | grep SSP_GA_m31 | cut -d ',' -f 1)"
CATID[0]="10092"                        # GA science
OBJID[0]="0x100000100000000-0x10000FFFFFFFFFF"

# Flux standards of the same visits

PROPOSAL[1]=${PROPOSAL[0]}
RERUN[1]=${RERUN[0]}
OBSLOGS[1]=${OBSLOGS[0]}
ASSIGNMENTS[1]=${ASSIGNMENTS[0]}
VISITS[1]=${VISITS[0]}
CATID[1]="3006"                         # Flux standards
OBJID[1]=""
