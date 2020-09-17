set -e

# releash.sh  -c [armv6l | arm64 | amd64 | 386] 
#             -o [linux | darwin | windows]  

# set parameters by default value
cpuType=amd64    # [armv6l | arm64 | amd64 | 386]
osType=linux   # [linux | darwin | windows]
version=""
declare -A archMap=(["armv6l"]="arm" ["arm64"]="arm64" ["amd64"]="x64" ["386"]="x86")
while getopts "h:c:o:n:" arg
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
    n)
      #echo "version=$OPTARG"
      version=$(echo $OPTARG)
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

if [ "$version" == "" ]; then 
  echo "Please input the correct version!"
  exit 1
fi

startdir=$(pwd)
scriptdir=$(dirname $(readlink -f $0))
cd ${scriptdir}/cmd/alert

echo "cpuType=${cpuType}"
echo "osType=${osType}"
echo "version=${version}"

GOOS=${osType} GOARCH=${cpuType} go build -ldflags '-X main.version='${version}

mkdir -p TDengine-alert/driver

cp alert alert.cfg install_driver.sh ./TDengine-alert/.
cp ../../../debug/build/lib/libtaos.so.${version} ./TDengine-alert/driver/.
chmod 777 ./TDengine-alert/install_driver.sh

tar -I 'gzip -9' -cf ${startdir}/TDengine-alert-${version}-${osType^}-${archMap[${cpuType}]}.tar.gz TDengine-alert/
rm -rf ./TDengine-alert

