#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -t template file"
    echo -e "\t -c config file"
    echo -e "\t -o output file"
    echo -e "\t -h help"
}

while getopts "c:t:o:h" opt; do
    case $opt in
        t)
            template_file=$OPTARG
            ;;
        c)
            config_file=$OPTARG
            ;;
        o)
            output_file=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 1
            ;;
    esac
done

if [ -z $template_file ]; then
    echo "template file not specified"
    usage
    exit 1
fi
if [ -z $config_file ]; then
    echo "config file not specified"
    usage
    exit 1
fi
if [ -z $output_file ]; then
    echo "output file not specified"
    usage
    exit 1
fi
if [ ! -f $template_file ]; then
    echo "template file $template_file not exist"
    usage
    exit 1
fi
if [ ! -f $config_file ]; then
    echo "config file $config_file not exist"
    usage
    exit 1
fi
cp -f $template_file $output_file
while read line; do
    if [ -z "$line" ]; then
        continue
    fi
    echo "$line"|grep -q "^#"
    if [ $? -eq 0 ]; then
        continue
    fi
    key=`echo "$line"|cut -d= -f1`
    value=`echo "$line"|cut -d= -f2`
    sed -i "s/\<$key\>/$value/g" $output_file
done <$config_file

cat $output_file
