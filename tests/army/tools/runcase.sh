set -e
count=0 
for i in  `find tools/benchmark/basic/ -name "*.py"`
     do printf "cnt=$count python3 test.py -f $i \n"
     python3 test.py -f $i
     ((count=count+1))
done

echo "benchmark/basic count=$count \n"