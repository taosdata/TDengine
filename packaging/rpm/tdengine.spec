%define homepath         /usr/local/taos
%define userlocalpath    /usr/local
%define cfg_install_dir  /etc/taos
%define __strip /bin/true
%global __python /usr/bin/python3
%global _build_id_links none

Name:		tdengine
Version:	%{_version}
Release:	3%{?dist}
Summary:	tdengine from taosdata
Group:	  Application/Database
License:	AGPL
URL:		  www.taosdata.com
AutoReqProv: no

#BuildRoot:  %_topdir/BUILDROOT
BuildRoot:   %{_tmppath}/%{name}-%{version}-%{release}-root

#Prefix: /usr/local/taos

#BuildRequires:
#Requires:

%description
Big Data Platform Designed and Optimized for IoT

#"prep" Nothing needs to be done
#%prep
#%setup -q
#%setup -T

#"build" Nothing needs to be done
#%build
#%configure
#make %{?_smp_mflags}

%install
#make install DESTDIR=%{buildroot}
rm -rf %{buildroot}

echo topdir: %{_topdir}
echo version: %{_version}
echo buildroot: %{buildroot}

libfile="libtaos.so.%{_version}"
wslibfile="libtaosws.so"

# create install path, and cp file
mkdir -p %{buildroot}%{homepath}/bin
mkdir -p %{buildroot}%{homepath}/cfg
#mkdir -p %{buildroot}%{homepath}/connector
mkdir -p %{buildroot}%{homepath}/driver
mkdir -p %{buildroot}%{homepath}/examples
mkdir -p %{buildroot}%{homepath}/include
#mkdir -p %{buildroot}%{homepath}/init.d
mkdir -p %{buildroot}%{homepath}/script

cp %{_compiledir}/../packaging/cfg/taos.cfg         %{buildroot}%{homepath}/cfg
if [ -f %{_compiledir}/test/cfg/taosadapter.toml ]; then
    cp %{_compiledir}/test/cfg/taosadapter.toml         %{buildroot}%{homepath}/cfg
fi
if [ -f %{_compiledir}/test/cfg/taosadapter.service ]; then
    cp %{_compiledir}/test/cfg/taosadapter.service %{buildroot}%{homepath}/cfg
fi

if [ -f %{_compiledir}/../build-taoskeeper/config/taoskeeper.toml ]; then
    cp %{_compiledir}/../build-taoskeeper/config/taoskeeper.toml %{buildroot}%{homepath}/cfg ||:
fi

if [ -f %{_compiledir}/../build-taoskeeper/taoskeeper.service ]; then
    cp %{_compiledir}/../build-taoskeeper/taoskeeper.service %{buildroot}%{homepath}/cfg ||:
fi

#cp %{_compiledir}/../packaging/rpm/taosd            %{buildroot}%{homepath}/init.d
cp %{_compiledir}/../packaging/tools/post.sh        %{buildroot}%{homepath}/script
cp %{_compiledir}/../packaging/tools/preun.sh       %{buildroot}%{homepath}/script
cp %{_compiledir}/../packaging/tools/startPre.sh    %{buildroot}%{homepath}/bin
cp %{_compiledir}/../packaging/tools/set_core.sh    %{buildroot}%{homepath}/bin
cp %{_compiledir}/../packaging/tools/taosd-dump-cfg.gdb    %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/taos                    %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/taosd                   %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/udfd                    %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/taosBenchmark           %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/taosdump                %{buildroot}%{homepath}/bin

if [ -f %{_compiledir}/../build-taoskeeper/taoskeeper ]; then
    cp %{_compiledir}/../build-taoskeeper/taoskeeper %{buildroot}%{homepath}/bin
fi

if [ -f %{_compiledir}/build/bin/taosadapter ]; then
    cp %{_compiledir}/build/bin/taosadapter                    %{buildroot}%{homepath}/bin
fi
cp %{_compiledir}/build/lib/${libfile}              %{buildroot}%{homepath}/driver
[ -f %{_compiledir}/build/lib/${wslibfile} ] && cp %{_compiledir}/build/lib/${wslibfile}            %{buildroot}%{homepath}/driver ||:
cp %{_compiledir}/../include/client/taos.h          %{buildroot}%{homepath}/include
cp %{_compiledir}/../include/common/taosdef.h       %{buildroot}%{homepath}/include
cp %{_compiledir}/../include/util/taoserror.h       %{buildroot}%{homepath}/include
cp %{_compiledir}/../include/util/tdef.h       %{buildroot}%{homepath}/include
cp %{_compiledir}/../include/libs/function/taosudf.h       %{buildroot}%{homepath}/include
[ -f %{_compiledir}/build/include/taosws.h ] && cp %{_compiledir}/build/include/taosws.h            %{buildroot}%{homepath}/include ||:
#cp -r %{_compiledir}/../src/connector/python        %{buildroot}%{homepath}/connector
#cp -r %{_compiledir}/../src/connector/go            %{buildroot}%{homepath}/connector
#cp -r %{_compiledir}/../src/connector/nodejs        %{buildroot}%{homepath}/connector
#cp %{_compiledir}/build/lib/taos-jdbcdriver*.*      %{buildroot}%{homepath}/connector ||:
cp -r %{_compiledir}/../examples/*                  %{buildroot}%{homepath}/examples

if [ -f %{_compiledir}/build/bin/jemalloc-config ]; then
    mkdir -p %{buildroot}%{homepath}/jemalloc/ ||:
    mkdir -p %{buildroot}%{homepath}/jemalloc/include/jemalloc/ ||:
    mkdir -p %{buildroot}%{homepath}/jemalloc/lib/ ||:
    mkdir -p %{buildroot}%{homepath}/jemalloc/lib/pkgconfig ||:

    cp %{_compiledir}/build/bin/jemalloc-config %{buildroot}%{homepath}/jemalloc/bin
    if [ -f %{_compiledir}/build/bin/jemalloc.sh ]; then
        cp %{_compiledir}/build/bin/jemalloc.sh %{buildroot}%{homepath}/jemalloc/bin
    fi
    if [ -f %{_compiledir}/build/bin/jeprof ]; then
        cp %{_compiledir}/build/bin/jeprof %{buildroot}%{homepath}/jemalloc/bin
    fi
    if [ -f %{_compiledir}/build/include/jemalloc/jemalloc.h ]; then
        cp %{_compiledir}/build/include/jemalloc/jemalloc.h %{buildroot}%{homepath}/jemalloc/include/jemalloc/
    fi
    if [ -f %{_compiledir}/build/lib/libjemalloc.so.2 ]; then
        cp %{_compiledir}/build/lib/libjemalloc.so.2 %{buildroot}%{homepath}/jemalloc/lib
        ln -sf libjemalloc.so.2 %{buildroot}%{homepath}/jemalloc/lib/libjemalloc.so
    fi
#    if [ -f %{_compiledir}/build/lib/libjemalloc.a ]; then
#        cp %{_compiledir}/build/lib/libjemalloc.a %{buildroot}%{homepath}/jemalloc/lib
#    fi
#    if [ -f %{_compiledir}/build/lib/libjemalloc_pic.a ]; then
#        cp %{_compiledir}/build/lib/libjemalloc_pic.a %{buildroot}%{homepath}/jemalloc/lib
#    fi
    if [ -f %{_compiledir}/build/lib/pkgconfig/jemalloc.pc ]; then
        cp %{_compiledir}/build/lib/pkgconfig/jemalloc.pc %{buildroot}%{homepath}/jemalloc/lib/pkgconfig
    fi
fi
ls -al %{buildroot}%{homepath}/bin
tree -L 5
echo "==============================copying files done"
#Scripts executed before installation
%pre
if [ -f /var/lib/taos/dnode/dnodeCfg.json ]; then
  echo -e "The default data directory \033[41;37m/var/lib/taos\033[0m contains old data of tdengine 2.x, please clear it before installing!"
  exit 1
fi
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# Stop the service if running
if pidof taosd &> /dev/null; then
    if pidof systemd &> /dev/null; then
        ${csudo}systemctl stop taosd || :
    elif $(which service  &> /dev/null); then
        ${csudo}service taosd stop || :
    else
        pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
        if [ -n "$pid" ]; then
           ${csudo}kill -9 $pid   || :
        fi
    fi
    echo "Stop taosd service success!"
    sleep 1
fi
# if taos.cfg already exist, remove it
if [ -f %{cfg_install_dir}/taos.cfg ]; then
    ${csudo}rm -f %{cfg_install_dir}/cfg/taos.cfg   || :
fi

# if taosadapter.toml already exist, remove it
if [ -f %{cfg_install_dir}/taosadapter.toml ]; then
    ${csudo}rm -f %{cfg_install_dir}/cfg/taosadapter.toml || :
fi

# there can not libtaos.so*, otherwise ln -s  error
${csudo}rm -f %{homepath}/driver/libtaos*   || :

#Scripts executed after installation
%post
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi
cd %{homepath}/script
${csudo}./post.sh

# Scripts executed before uninstall
%preun
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi
# only remove package to call preun.sh, not but update(2)
if [ $1 -eq 0 ];then
  #cd %{homepath}/script
  #${csudo}./preun.sh

  if [ -f %{homepath}/script/preun.sh ]; then
    cd %{homepath}/script
    ${csudo}./preun.sh
  else
    bin_link_dir="/usr/bin"
    lib_link_dir="/usr/lib"
    inc_link_dir="/usr/include"

    data_link_dir="/usr/local/taos/data"
    log_link_dir="/usr/local/taos/log"
    cfg_link_dir="/usr/local/taos/cfg"

    # Remove all links
    ${csudo}rm -f ${bin_link_dir}/taos       || :
    ${csudo}rm -f ${bin_link_dir}/taosd      || :
    ${csudo}rm -f ${bin_link_dir}/udfd       || :
    ${csudo}rm -f ${bin_link_dir}/taosadapter       || :
    ${csudo}rm -f ${bin_link_dir}/taoskeeper       || :
    ${csudo}rm -f ${cfg_link_dir}/*          || :
    ${csudo}rm -f ${inc_link_dir}/taos.h     || :
    ${csudo}rm -f ${inc_link_dir}/taosdef.h     || :
    ${csudo}rm -f ${inc_link_dir}/taoserror.h     || :
    ${csudo}rm -f ${inc_link_dir}/tdef.h     || :
    ${csudo}rm -f ${inc_link_dir}/taosudf.h     || :    
    ${csudo}rm -f ${lib_link_dir}/libtaos.*  || :

    ${csudo}rm -f ${log_link_dir}            || :
    ${csudo}rm -f ${data_link_dir}           || :

    pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
    if [ -n "$pid" ]; then
      ${csudo}kill -9 $pid   || :
    fi
  fi
fi

# Scripts executed after uninstall
%postun

# clean build dir
%clean
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi
${csudo}rm -rf %{buildroot}

#Specify the files to be packaged
%files
/*
#%doc

#Setting default permissions
%defattr  (-,root,root,0755)
#%{prefix}

#%changelog
