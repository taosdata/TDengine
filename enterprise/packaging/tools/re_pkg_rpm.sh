# /bin/bash
#
#    |---- newpkg
#    |---- pkgroom
#    |     |---- BUILDROOT
#    |           |---- tdengine-2.0.15.0-3.x86_64
#    |                 |---- usr/local/taos
#    |     |---- SPECS
#    |           |---- tdengine.spec
#    |---- rpmbuild
#    |---- TDengine-server-2.0.15.0-Linux-x64.rpm
#    |---- re_pkg_rpm.sh

emailInfo=$1
rpmPkg=$2

curDir=`pwd`
echo "==== current dir: ${curDir} ===="
#echo "==== input parameters: ===="
#echo "emailInfo: ${emailInfo}"
#echo "rpmPkg: ${rpmPkg}"
#echo

cp -f ${rpmPkg} ${curDir}/ ||:
pkgName=`basename $rpmPkg`
verNumber=${pkgName%-Linux*}
verNumber=${verNumber##*-}

#echo "rpmPkt:${rpmPkg}"
#echo "pkgName:${pkgName}"
#echo "verNumber:${verNumber}"

rm -rf pkgroom  ||:
mkdir -p pkgroom/BUILDROOT/tdengine-${verNumber}-3.x86_64
mkdir -p pkgroom/SPECS

rpm2cpio ${pkgName} | cpio -idv

# add email info
echo ${emailInfo} > usr/local/taos/email
mv usr pkgroom/BUILDROOT/tdengine-${verNumber}-3.x86_64/
./rpmbuild/rpmrebuild.sh -s tdengine.spec -p ${pkgName}

# add new email file into spec
#%attr(0755, root, root) "/usr/local/taos/email"

mv tdengine.spec pkgroom/SPECS

rpmbuild --define="_topdir ${curDir}/pkgroom" -ba ${curDir}/pkgroom/SPECS/tdengine.spec

rm -f ${pkgName}
rm -rf newpkg ||:
mkdir newpkg
mv pkgroom/RPMS/x86_64/*.rpm newpkg/${pkgName}


