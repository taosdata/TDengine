set -e
count=0 
for i in  `find benchmark/basic/ -name "*.py"`
     do printf ",,y,army,./pytest.sh python3 ./test.py -f tools/$i\n"
     ((count=count+1))
done

for i in  `find benchmark/cloud/ -name "*.py"`
     do printf ",,y,army,./pytest.sh python3 ./test.py -f tools/$i\n"
     ((count=count+1))
done

for i in  `find benchmark/ws/ -name "*.py"`
     do printf ",,y,army,./pytest.sh python3 ./test.py -f tools/$i\n"
     ((count=count+1))
done


printf "\nbenchmark count=$count \n"


for i in  `find taosdump/native/ -name "*.py"`
     do printf ",,y,army,./pytest.sh python3 ./test.py -f tools/$i\n"
     ((count=count+1))
done

for i in  `find taosdump/ws/ -name "*.py"`
     do printf ",,y,army,./pytest.sh python3 ./test.py -f tools/$i\n"
     ((count=count+1))
done


printf "\nall count=$count \n"