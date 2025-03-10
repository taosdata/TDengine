#
# copy this file to tests/army/ folder to running 
#
set -e
count=0
nskip=1000
for i in  `find tools/benchmark/basic/ -name "*.py"`
     do printf "\n\n ***** cnt=$count  python3 test.py -f $i  *****\n\n"
     if [ "$count" -lt "$nskip" ]; then
        printf "skip $count ... \n"
        ((count=count+1))
	continue  # less than nskip, contine
     fi
     python3 test.py -f $i
     ((count=count+1))
done

echo "benchmark/basic count=$count \n"


for i in  `find tools/taosdump/native/ -name "*.py"`
     do printf "\n\n ***** cnt=$count  python3 test.py -f $i  *****\n\n"
     python3 test.py -f $i
     ((count=count+1))
done

