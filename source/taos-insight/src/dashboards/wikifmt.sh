#!/usr/bin/env bash
wiki=$1
# for `# * * ` level-3
perl -0777 -i.step1 -pe 's/\# \* \* /\#\*\* /igs' $wiki
# for `# * ` level-2
perl -0777 -i.step1 -pe 's/\# \* /\#\* /igs' $wiki
# for `* * * ` level-3
perl -0777 -i.step1 -pe 's/\& \* \* /\*\*\* /igs' $wiki
# for `* * ` level-2
perl -0777 -i.step1 -pe 's/\* \* /\*\* /igs' $wiki
perl -0777 -i.step1 -pe 's/\*  /\* /igs' $wiki

# clean
perl -0777 -i.step3 -pe 's/\n# \n# \n/\n/igs' $wiki
perl -0777 -i.step4 -pe 's/\n\* \n\* \n/\n/igs' $wiki
perl -0777 -i.step4 -pe 's/\n\#\* \n/\n/igs' $wiki

perl -0777 -i.step5 -pe 's/\.\..assets.//igs' $wiki