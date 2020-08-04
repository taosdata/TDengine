set -e

# releash.sh  -c [armv6l | arm64 | amd64 | 386] 
#             -o [linux | darwin | windows]  

# set parameters by default value
cpuType=amd64    # [armv6l | arm64 | amd64 | 386]
osType=linux   # [linux | darwin | windows]

declare -A archMap=(["armv6l"]="arm" ["arm64"]="arm64" ["amd64"]="x64" ["386"]="x86")
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
      echo "Usage: `basename $0` -c [armv6l | arm64 | amd64 | 386] -o [linux | darwin | windows]"
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
version=${version:1}

echo "cpuType=${cpuType}"
echo "osType=${osType}"
echo "version=${version}"

GOOS=${osType} GOARCH=${cpuType} go build

tar -I 'gzip -9' -cf ${startdir}/TDengine-alert-${version}-${osType^}-${archMap[${cpuType}]}.tar.gz alert alert.cfg install_driver.sh driver/
