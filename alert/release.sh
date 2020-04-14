set -e

# releash.sh  -c [arm | arm64 | x64 | x86] 
#             -o [linux | darwin | windows]  

# set parameters by default value
cpuType=x64    # [arm | arm64 | x64 | x86]
osType=linux   # [linux | darwin | windows]

while getopts "h:c:o:" arg
do
  case $arg in
    c)
      #echo "cpuType=$OPTARG"
      cpuType=$(echo $OPTARG)
      ;;
    o)
      #echo "osType=$OPTARG"
      osType=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -c [arm | arm64 | x64 | x86] -o [linux | darwin | windows]"
      exit 0
      ;;
    ?) #unknown option 
      echo "unknown argument"
      exit 1
      ;;
  esac
done


startdir=$(pwd)
scriptdir=$(dirname $(readlink -f $0))
cd ${scriptdir}/cmd/alert
version=$(grep 'const version =' main.go | awk '{print $NF}')
version=${version%\"}

echo "cpuType=${cpuType}"
echo "osType=${osType}"
echo "version=${version}"

GOOS=${osType} GOARCH=${cpuType} go build

GZIP=-9 tar -zcf ${startdir}/tdengine-alert-${version}-${osType}-${cpuType}.tar.gz alert alert.cfg install_driver.sh driver/
