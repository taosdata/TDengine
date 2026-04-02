#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK BAD KEY ID"
id_key --fail 0
expect_error EINVAL

# check non-existent standard IDs
marker "CHECK BAD IDS"
id_key --fail2 @wwww
id_key --fail2 @qqqq

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
