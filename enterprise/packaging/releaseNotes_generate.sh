#!/bin/bash

fromBranch=$1
toBranch=$2

scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=$scriptDir/../release


if [ ! -d $archiveDir ]; then
    mkdir -p $archiveDir
fi
#
echo "generate release notes>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir/
git log origin/$fromBranch..origin/$toBranch --pretty=format:'%b' > ./releaseNotes.tmp
#git log origin/$fromBranch..origin/$toBranch --pretty=format:'%s' > ./releaseNotes.tmp

# fix
echo "Release Notes" > ReleaseNotes
echo "" >> ReleaseNotes
# fix
echo "FIX BUGS:" >> ReleaseNotes
grep '\[[tT][dD]-.*\] *<hotfix>:' ./releaseNotes.tmp | sed 's/ *<hotfix>//' | awk -F: '{print $1 ":" $2}' >> ReleaseNotes
grep '\[[tT][dD]-.*\] *<fix>:' ./releaseNotes.tmp | sed 's/ *<fix>//' | awk -F: '{print $1 ":" $2}' >> ReleaseNotes
echo "" >> ReleaseNotes

# enhance
echo "ENHANCEMENTS:" >> ReleaseNotes
grep '\[[tT][dD]-.*\] *<enhance>:' ./releaseNotes.tmp | sed 's/ *<enhance>//' | awk -F: '{print $1 ":" $2}' >> ReleaseNotes
echo "" >> ReleaseNotes

# feature
echo "NEW FEATURES:" >> ReleaseNotes
grep '\[[tT][dD]-.*\] *<feature>:' ./releaseNotes.tmp | sed 's/ *<feature>//' | awk -F: '{print $1 ":" $2}' >> ReleaseNotes
echo "" >> ReleaseNotes

# docs
echo "DOCS:" >> ReleaseNotes
grep '\[[tT][dD]-.*\] *<docs>:' ./releaseNotes.tmp | sed 's/ *<docs>//' | awk -F: '{print $1 ":" $2}' >> ReleaseNotes
echo "" >> ReleaseNotes

# test
echo "TEST:" >> ReleaseNotes
grep '\[[tT][dD]-.*\] *<test>:' ./releaseNotes.tmp | sed 's/ *<test>//' | awk -F: '{print $1 ":" $2}' >> ReleaseNotes
echo "" >> ReleaseNotes

rm -f ./releaseNotes.tmp
mv ./ReleaseNotes $scriptDir
cd $scriptDir
