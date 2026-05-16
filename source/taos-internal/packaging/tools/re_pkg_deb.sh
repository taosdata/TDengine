# /bin/bash
#
#    |---- build
#    |     |---- tdengine_2.0.20.0_amd64.deb
#    |     extract
#    |     |---- DEBIAN
#    |     |     |---- control,postinst,postrm,preinst,prerm
#    |     |---- usr/local/taos
#    |---- TDengine-server-2.0.14.0-Linux-x64.deb
#    |---- re_pkg_deb.sh

emailInfo=$1
debPkg=$2

curDir=`pwd`
echo "==== current dir: ${curDir} ===="
#echo "==== input parameters: ===="
#echo "emailInfo: ${emailInfo}"
#echo "debPkg: ${debPkg}"
#echo

rm -rf extract ||:
rm -rf build   ||:
rm -rf newpkg  ||:

cp ${debPkg} ${curDir}/
pkgName=`basename ${debPkg}`

mkdir -p extract/DEBIAN
mkdir -p build

dpkg -X ${pkgName} extract/
dpkg -e ${pkgName} extract/DEBIAN/

# add email info
echo ${emailInfo} > extract/usr/local/taos/email

dpkg-deb -b extract/ build/

#rm -f ${pkgName}
mkdir newpkg
mv build/*.deb newpkg/${pkgName}


