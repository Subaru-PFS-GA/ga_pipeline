#!/bin/bash

# Download a run from the Science Platform

SPURL="https://hscpfs.mtk.nao.ac.jp/fileaccess"
SPTOKEN="0NvBXoL-RVEtHt_uFymhknde6uAj4Uf9e793ebwbmrA"
DATADIR="/scratch/aszalay1/dobos/pfs"
OBSLOGDIR="/home/dobos/project/Subaru-PFS/spt_ssp_observation"

PROPOSAL="S25A-OT02"
RERUN="S25A_November2025"
OBSDATE="2025-0[3-6]"

# CATID=10092         # GA science
CATID=3006          # fluxstd
# CATID=1006          # sky GAIA
# CATID=1007          # sky PS1

export PFSSPEC_PFS_DATADIR="$DATADIR"
export PFSSPEC_PFS_RERUNDIR="$RERUN"
export PFSSPEC_PFS_RERUN="$RERUN"
export PFSSPEC_PFS_DESIGNDIR="$DATADIR/raw/pfsDesign"
export PFSSPEC_PFS_CONFIGDIR="$DATADIR/raw/pfsConfig"

export GAPIPE_DATADIR="$DATADIR/gapipe/data"
export GAPIPE_WORKDIR="$DATADIR/gapipe/work"
export GAPIPE_OUTDIR="$DATADIR/gapipe/out"

export BUTLER_CONFIGDIR="$DATADIR/ssp/$PROPOSAL/2d"
export BUTLER_COLLECTIONS="$RERUN"

# Create the necessary directories

mkdir -p ./tmp
mkdir -p $DATADIR/ssp/$PROPOSAL
mkdir -p $DATADIR/ssp/$PROPOSAL/2d/$RERUN
mkdir -p $DATADIR/ssp/$PROPOSAL/2d/$RERUN/pfsConfig

# Get the list of visits from the observation logs
VISITS="$(cat $OBSLOGDIR/runs/$OBSDATE/obslog/*.csv | grep SSP_GA | cut -d ',' -f 1)"
echo Visits matching the filters: $VISITS

# # Download the butler files
# wget -k -np --header="Authorization: Bearer $SPTOKEN" $SPURL/pfs/programs/$PROPOSAL/2d/butler.yaml -O $DATADIR/ssp/$PROPOSAL/2d/butler.yaml
# wget -k -np --header="Authorization: Bearer $SPTOKEN" $SPURL/pfs/programs/$PROPOSAL/2d/gen3.sqlite3 -O $DATADIR/ssp/$PROPOSAL/2d/gen3.sqlite3

echo "The following collections are available in the butler repo:"
python <<EOF
from lsst.daf.butler import Butler
butler = Butler('$DATADIR/ssp/$PROPOSAL/2d', writeable=False)
print(butler.registry.queryCollections())
EOF
echo "Processing collection $RERUN"

# Search and show information about PFS data products
function gapipe-repo() {
    `realpath ./bin/wrap` "-m pfs.ga.pipeline.scripts.repo.reposcript" "$@"
}

# # Generate the list of PfsConfig files to download
# # Include all PfsConfig files for the selected visits
# gapipe-repo find-product PfsConfig --visit $VISITS --format path \
#     | sed "s|$DATADIR/ssp/||g" \
#     > ./tmp/pfsconfig_paths.txt

# echo Found $(cat ./tmp/pfsconfig_paths.txt | wc -l) PfsConfig files to download.

# # Download the PfsConfig files
# wget -i ./tmp/pfsconfig_paths.txt \
#     --header="Authorization: Bearer $SPTOKEN" \
#     --base $SPURL/pfs/programs/ \
#     --continue \
#     -P $DATADIR/ssp --no-host-directories --cut-dirs=3 -x

# # Get the list of PfsCalibrated files available based on Butler
# gapipe-repo find-product PfsCalibrated --visit $VISITS --format path \
#     | sed "s|$DATADIR/ssp/||g" \
#     > ./tmp/pfscalibrated_paths.txt

# echo Found $(cat ./tmp/pfscalibrated_paths.txt | wc -l) PfsCalibrated files to download.

# # Schedule jobs to download the PfsCalibrated files
# while IFS= read -r filepath; do
#     cat > ./tmp/download_script.sh <<EOF
# #!/bin/bash
# mkdir -p "\$(dirname "$DATADIR/ssp/$filepath")"
# wget -O "$DATADIR/ssp/$filepath" -x \
#     --header="Authorization: Bearer $SPTOKEN" \
#     --continue \
#     $SPURL/pfs/programs/$filepath
# EOF

#     sbatch --partition=cpu --job-name=download_pfscalibrated ./tmp/download_script.sh
# done < ./tmp/pfscalibrated_paths.txt

# echo Download jobs submitted via sbatch.

# Extract PfsSingle from PfsCalibrated
gapipe-repo extract-product PfsCalibrated,PfsSingle \
    --visit $VISITS \
    --catid $CATID \
    --rerundir $RERUN \
    --log-level DEBUG \
    --batch slurm --partition cpu --cpus 2 --memory 2G
