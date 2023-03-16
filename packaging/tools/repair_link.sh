#!/bin/bash

# This script is used to repaire links when you what to move TDengine
# data to other places and to access data.

# Read link path
read -p "Please enter link directory such as /var/lib/taos/tsdb: " linkDir

while true; do
    if [ ! -d $linkDir ]; then
        read -p "Path not exists, please enter the correct link path:" linkDir
        continue
    fi
    break
done

declare -A dirHash

for linkFile in $(find -L $linkDir -xtype l); do
    targetFile=$(readlink -f $linkFile)
    echo "targetFile: ${targetFile}"
    # TODO : Extract directory part and basename part
    dirName=$(dirname $(dirname ${targetFile}))
    baseName=$(basename $(dirname ${targetFile}))/$(basename ${targetFile})

    # TODO : 
    newDir="${dirHash["$dirName"]}"
    if [ -z "${dirHash["$dirName"]}" ]; then
        read -p "Please enter the directory to replace ${dirName}:" newDir

        read -p "Do you want to replace all[y/N]?" replaceAll
        if [[ ( "${replaceAll}" == "y") || ( "${replaceAll}" == "Y") ]]; then
            dirHash["$dirName"]="$newDir"
        fi
    fi

    # Replace the file
    ln -sf "${newDir}/${baseName}" "${linkFile}"
done
