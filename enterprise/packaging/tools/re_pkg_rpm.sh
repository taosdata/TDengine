# bash
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

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
  echo "### something wrong!"
  exit 1
}

emailInfo=$1
rpmPkg=$2

curDir=`pwd`

# need create pkg_stage and set right permission
stageDir=${curDir}/pkg_stage

#echo "==== current dir: ${curDir} ===="
#echo "==== input parameters: ===="
#echo "emailInfo: ${emailInfo}"
#echo "rpmPkg: ${rpmPkg}"
#echo

pkgName=`basename $rpmPkg`
verNumber=${pkgName%-Linux*}
verNumber=${verNumber##*-}

#echo "rpmPkt:${rpmPkg}"
#echo "pkgName:${pkgName}"
#echo "verNumber:${verNumber}"

cp -f ${rpmPkg} ${stageDir} && echo "cp ${rpmPkg} ${stageDir} done." || echo "cp ${rpmPkg} ${stageDir} failed"

cd ${stageDir}

rm -rf pkgroom  ||:
mkdir -p pkgroom/BUILDROOT/tdengine-${verNumber}-3.x86_64
mkdir -p pkgroom/SPECS

rpm2cpio ${pkgName} | cpio -idv

# add email info
echo ${emailInfo} > usr/local/taos/email

mv usr pkgroom/BUILDROOT/tdengine-${verNumber}-3.x86_64/

${stageDir}/rpmbuild/rpmrebuild.sh -s tdengine.spec -p ${pkgName}

# add new email file into spec
#%attr(0755, root, root) "/usr/local/taos/email"

sed -i "s/taosdump\"/taosdump\"\n%attr(0755, root, root) \"\/usr\/local\/taos\/email\"/g" tdengine.spec
echo "### LN69 ###"
mv tdengine.spec pkgroom/SPECS

rpmbuild --define="_topdir ${stageDir}/pkgroom" -ba ${stageDir}/pkgroom/SPECS/tdengine.spec

newpkgDir=newpkg-${emailInfo}

if [ -d ${newpkgDir} ];
then
  rm -rf ${newpkgDir} ||:
  mkdir ${newpkgDir}
fi

mv pkgroom/RPMS/x86_64/*.rpm ${newpkgDir}/${pkgName}

echo " ==== Done ===="
