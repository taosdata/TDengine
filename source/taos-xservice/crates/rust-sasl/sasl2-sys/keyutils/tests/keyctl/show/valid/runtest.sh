#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# add some keyrings, nested one inside the other
nr_keyrings=7
keyrings=
parent=@s
for ((i=1; i<=$nr_keyrings; i++))
do
    marker "ADD KEYRING $i"
    create_keyring --new=keyringid wibble$i $parent
    parent=$keyringid
    keyrings="$keyrings $keyringid"
done

# check that show works
marker "SHOW SESSION KEYRING"
keyctl show >>$OUTPUTFILE 2>&1
if [ $? != 0 ]
then
    failed
fi

if [ $OSDIST = RHEL ] && ! version_less_than $OSRELEASE 6.6 ||
   keyutils_at_or_later_than 1.5.6
then
    # should be eight lines in the output (banner + session + 6 keyrings)
    marker "COUNT LINES"
    nlines=`keyctl show | wc -l`
    if [ "$nlines" -ne $(($nr_keyrings + 2)) ]
    then
	failed
    fi

    # check the key ID list
    marker "CHECK KEY ID LIST"
    keyids=`keyctl show | tail -n +3 | cut -c1-11`

    # we need to fix up the whitespace
    keyids=`echo $keyids`
    keyrings=`echo $keyrings`

    echo "Compare '$keyids'" >>$OUTPUTFILE
    echo "And     '$keyrings'" >>$OUTPUTFILE
    if [ "$keyids" != "$keyrings" ]
    then
	failed
    fi
fi

# check that shows of specified keyrings also work
if keyutils_at_or_later_than 1.5.4
then
    declare -i j
    j=$nr_keyrings
    for i in $keyrings
    do
	marker "CHECK SHOW OTHERS $j"
	echo --- $i >>$OUTPUTFILE
	if ! keyctl show $i >>$OUTPUTFILE
	then
	    failed
	fi
	k=`keyctl show $i | wc -l`
	if [ $(($j + 1)) != $k ]
	then
	    failed
	fi
	j=$(($j - 1))
    done
fi
echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
