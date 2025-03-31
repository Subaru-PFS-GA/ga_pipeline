#!/bin/bash

filelist=$(realpath "$1")

SOURCE_DIR="/work/datastore"

TARGET_SERVER="dobos@dslogin01.pha.jhu.edu"
TARGET_DIR="/datascope/subaru/data/commissioning/gen3"

pushd "$SOURCE_DIR"

rsync -avR --files-from=$filelist / $TARGET_SERVER:$TARGET_DIR/

popd

# #!/bin/bash
# while IFS= read -r line; do
#     # Make source filename relative to SOURCE_DIR
#     relpath=$(realpath --relative-to="$SOURCE_DIR" "$line")
#     # Get the directory portion and the file name from the path
#     filename=$(basename "$relpath")
#     dir=$(dirname "$relpath")

#     # Create the target directory and copy the file
#     # -n prevents ssh from hijacking stdin
#     ssh -n $TARGET_SERVER mkdir -p $TARGET_DIR/$dir
#     scp -B -p "$SOURCE_DIR/$dir/$filename" "$TARGET_SERVER:$TARGET_DIR/$dir/"
# done < "$filelist"

# rsync -av --files-from=pfsConfig.list / dobos@dslogin01.pha.jhu.edu:/datascope/subaru/data/commissioning/gen3/

# ssh volta04 mkdir -p /datascope/subaru/data/commissioning/gen3/PFS/raw/pfsConfig/pfsConfig/20250124/119885/

# scp /work/datastore/PFS/raw/pfsConfig/pfsConfig/20250124/119885/pfsConfig_PFS_119885_PFS_raw_pfsConfig.fits volta04:/datascope/subaru/data/commissioning/gen3/PFS/raw/pfsConfig/pfsConfig/20250124/119885/pfsConfig_PFS_119885_PFS_raw_pfsConfig.fits