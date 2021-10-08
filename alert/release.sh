set -e

# releash.sh  -c [armv6l | arm64 | amd64 | 386] 
#             -o [linux | darwin | windows]  

# set parameters by default value
cpuType=amd64    # [armv6l | arm64 | amd64 | 386]
osType=linux   # [linux | darwin | windows]
version=""
verType=stable   # [stable, beta]
declare -A archMap=(["armv6l"]="arm" ["arm64"]="arm64" ["amd64"]="x64" ["386"]="x86")
while getopts "h:c:o:n:V:" arg
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
     V)
      #echo "verType=$OPTARG"
      verType=$(echo $OPTARG)
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

tar -I 'gzip -9' -cf ${scriptdir}/TDengine-alert-${version}-${osType^}-${archMap[${cpuType}]}.tar.gz TDengine-alert/
rm -rf ./TDengine-alert



# mv package to comminuty/release/
pkg_name=TDengine-alert-${version}-${osType^}-${archMap[${cpuType}]}

if [ "$verType" == "beta" ]; then
  pkg_name=TDengine-alert-${version}-${verType}-${osType^}-${archMap[${cpuType}]}
elif [ "$verType" == "stable" ]; then
  pkg_name=${pkg_name}
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

cd ${scriptdir}/../release/
mv ${scriptdir}/TDengine-alert-${version}-${osType^}-${archMap[${cpuType}]}.tar.gz  ${pkg_name}.tar.gz
