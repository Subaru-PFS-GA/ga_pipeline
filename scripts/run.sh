# NOTE: this script isn't meant to be run as a whole. Instead, copy and
# paste the relevant sections to run them step by step.

OBSDATE="2025-03"
RERUN="run21_June2025"
OBSLOGS="../Subaru-PFS/spt_ssp_observation/runs/$OBSDATE/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

CATID="10092"
OBJID="0x200000000-0x2FFFFFFFF"     # Draco dSph

CATID="10092"
OBJID="0x600000000-0x6FFFFFFFF"     # Ursa Minor dSph

CATID="3006"                        # Flux standards
OBJID="0x00000000-0xFFFFFFFF"

gapipe-configure \
    --config ./configs/gapipe/$RERUN/single.py \
    --visit $VISITS \
    --obs-logs $OBSLOGS \
    --catid $CATID

gapipe-run \
    --rerundir $RERUN \
    --catid $CATID \
    --objid $OBJID \
    --batch slurm --partition v100 --cpus 2 --mem 12G


###

OBSDATE="2025-05"
RERUN="run22_July2025"
OBSLOGS="../Subaru-PFS/spt_ssp_observation/runs/$OBSDATE/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

CATID="10092"
OBJID="0x1000000000-0x10FFFFFFFF"       # ID_PREFIX_OD_L90_B28
OBJID="0x2000000000-0x20FFFFFFFF"       # ID_PREFIX_OD_L90_B29

CATID="3006"                        # Flux standards
OBJID="0x00000000-0xFFFFFFFF"

gapipe-configure \
    --config ./configs/gapipe/$RERUN/single.py \
    --visit $VISITS \
    --obs-logs $OBSLOGS \
    --catid $CATID

ls -1 | cut -c1-8 | sort | uniq

gapipe-run \
    --rerundir $RERUN \
    --batch slurm --partition v100 --cpus 2 --mem 12G


###

OBSDATE="2025-06"
RERUN="run23_August2025"
OBSLOGS="../Subaru-PFS/spt_ssp_observation/runs/$OBSDATE/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

CATID="10092"

OBJID="0x0200000000-0x02FFFFFFFF" # SSP_GA_dra
OBJID="0x0600000000-0x06FFFFFFFF" # SSP_GA_umi
OBJID="0x7000000000-0x70FFFFFFFF" # SSP_GA_outerdisk_l90_b16
OBJID="0x3000000000-0x30FFFFFFFF" # SSP_GA_outerdisk_l90_bm28

CATID="3006"                        # Flux standards
OBJID="0x00000000-0xFFFFFFFF"

gapipe-configure \
    --config ./configs/gapipe/$RERUN/single.py \
    --visit $VISITS \
    --obs-logs $OBSLOGS \
    --catid $CATID

ls -1 | cut -c1-8 | sort | uniq

gapipe-run \
    --rerundir $RERUN \
    --batch slurm --partition v100 --cpus 2 --mem 12G


###

RUN="S25B-OT02"
RERUN="run24_November2025"
OBSDATE="2025-09"

export BUTLER_CONFIGDIR="/scratch/aszalay1/dobos/pfs/ssp/S25B-OT02/2d"
export BUTLER_COLLECTIONS="$RERUN"
export GAPIPE_RERUNDIR="$RERUN"
export GAPIPE_RERUN="$RERUN"

OBSLOGS="/home/dobos/project/Subaru-PFS/spt_ssp_observation/runs/$OBSDATE/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

CATID=10092         # GA science


OBJID="0x100000100000000-0x1000004FFFFFFFF"        # All M31

OBJID="0x100000100000000-0x1000001FFFFFFFF"        M31 E0
OBJID="0x100000200000000-0x1000002FFFFFFFF"        M31 W0
OBJID="0x100000300000000-0x1000003FFFFFFFF"        M31 GSS0
OBJID="0x100000400000000-0x1000004FFFFFFFF"        M31 NWS0

gapipe-repo find-object --visit $VISITS --objid $OBJID

gapipe-configure \
    --config ./configs/gapipe/$RERUN/single.py \
    --visit $VISITS \
    --obs-logs $OBSLOGS \
    --catid $CATID

gapipe-run \
    --rerundir $RERUN \
    --batch slurm --partition cpu --cpus 4 --mem 12G

gapipe-catalog \
    --rerundir $RERUN \
    --visit $VISITS \
    --obs-log $OBSLOGS \
    --objid $OBJID


# 17:51:50 gapipe INFO Saving catalog to `/scratch/aszalay1/dobos/pfs/gapipe/out/run24_November2025/pfsStarCatalog/10092/076-0x6e8ce8830dab14c9/pfsStarCatalog-10092-076-0x6e8ce8830dab14c9.fits`

### ---   S25A_November2025   --- ###

# Note that dSph data from different runs should not be combined into a single catalog!

export PROPOSAL="S25A-OT02"
export RERUN="S25A_November2025"
export OBSDATE="2025-0[3-6]"

export BUTLER_CONFIGDIR="/scratch/aszalay1/dobos/pfs/ssp/$PROPOSAL/2d"
export BUTLER_COLLECTIONS="$RERUN"
export GAPIPE_RERUNDIR="$RERUN"
export GAPIPE_RERUN="$RERUN"

OBSLOGS="/home/dobos/project/Subaru-PFS/spt_ssp_observation/runs/$OBSDATE/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

echo $VISITS

CATID=10092         # GA science

gapipe-repo find-object --visit $VISITS --objid $OBJID

gapipe-configure \
    --config ./configs/gapipe/$RERUN/single.py \
    --visit $VISITS \
    --obs-logs $OBSLOGS \
    --catid $CATID

gapipe-run \
    --rerundir $RERUN \
    --batch slurm --partition cpu --cpus 4 --mem 12G

# Generate catalogs

export OBSDATE="2025-03"
# export OBSDATE="2025-05"
# export OBSDATE="2025-06"

OBSLOGS="/home/dobos/project/Subaru-PFS/spt_ssp_observation/runs/$OBSDATE/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

OBJID="0x0200000000-0x02FFFFFFFF" # SSP_GA_dra
OBJID="0x0600000000-0x06FFFFFFFF" # SSP_GA_umi
OBJID="0x7000000000-0x70FFFFFFFF" # SSP_GA_outerdisk_l90_b16
OBJID="0x3000000000-0x30FFFFFFFF" # SSP_GA_outerdisk_l90_bm28

OBJID="0x100000100000000-0x1000004FFFFFFFF"        # All M31

OBJID="0x100000100000000-0x1000001FFFFFFFF"        M31 E0
OBJID="0x100000200000000-0x1000002FFFFFFFF"        M31 W0
OBJID="0x100000300000000-0x1000003FFFFFFFF"        M31 GSS0
OBJID="0x100000400000000-0x1000004FFFFFFFF"        M31 NWS0

gapipe-catalog \
    --rerundir $RERUN \
    --visit $VISITS \
    --obs-log $OBSLOGS \
    --objid $OBJID