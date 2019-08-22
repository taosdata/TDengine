%define homepath         /usr/local/taos
%define cfg_install_dir  /etc/taos

Name:		  tdengine
Version:	%{_version}
Release:	3%{?dist}
Summary:	tdengine from taosdata
Group:	  Application/Database
License:	AGPL
URL:		  www.taosdata.com

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

versioninfo=$(%{_compiledir}/../packaging/tools/get_version.sh)
libfile="libtaos.so.${versioninfo}"

# create install path, and cp file
mkdir -p %{buildroot}%{homepath}/bin
mkdir -p %{buildroot}%{homepath}/cfg
mkdir -p %{buildroot}%{homepath}/connector
mkdir -p %{buildroot}%{homepath}/driver
mkdir -p %{buildroot}%{homepath}/examples
mkdir -p %{buildroot}%{homepath}/include
mkdir -p %{buildroot}%{homepath}/init.d
mkdir -p %{buildroot}%{homepath}/script

cp %{_compiledir}/../packaging/cfg/taos.cfg         %{buildroot}%{homepath}/cfg
cp %{_compiledir}/../packaging/rpm/taosd            %{buildroot}%{homepath}/init.d
cp %{_compiledir}/../packaging/tools/post.sh        %{buildroot}%{homepath}/script
cp %{_compiledir}/../packaging/tools/preun.sh       %{buildroot}%{homepath}/script
cp %{_compiledir}/build/bin/taos                    %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/taosd                   %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/taosdemo                %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/taosdump                %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/lib/${libfile}              %{buildroot}%{homepath}/driver
cp %{_compiledir}/../src/inc/taos.h                 %{buildroot}%{homepath}/include
cp -r %{_compiledir}/../src/connector/grafana       %{buildroot}%{homepath}/connector
cp -r %{_compiledir}/../src/connector/python        %{buildroot}%{homepath}/connector
cp -r %{_compiledir}/../src/connector/go            %{buildroot}%{homepath}/connector
cp %{_compiledir}/build/lib/taos-jdbcdriver*dist.*  %{buildroot}%{homepath}/connector
cp -r %{_compiledir}/../tests/examples/*            %{buildroot}%{homepath}/examples

#Scripts executed before installation
%pre
function is_using_systemd() {
    if pidof systemd &> /dev/null; then
        return 0
    else
        return 1
    fi
}

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi

# Stop the service if running
if pidof taosd &> /dev/null; then
    if is_using_systemd; then
        ${csudo} systemctl stop taosd || :
    else
        ${csudo} service taosd stop || :
    fi
    echo "Stop taosd service success!"
    sleep 1
fi

# if taos.cfg already softlink, remove it
if [ -f %{cfg_install_dir}/taos.cfg ]; then
    ${csudo} rm -f %{homepath}/cfg/taos.cfg   || :
fi 

#Scripts executed after installation
%post
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi
cd %{homepath}/script
${csudo} ./post.sh
 
# Scripts executed before uninstall
%preun
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi
# only remove package to call preun.sh, not but update(2) 
if [ $1 -eq 0 ];then
  cd %{homepath}/script
  ${csudo} ./preun.sh
fi
 
# Scripts executed after uninstall
%postun
 
# clean build dir
%clean
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi
${csudo} rm -rf %{buildroot}

#Specify the files to be packaged
%files
/*
#%doc

#Setting default permissions
%defattr  (-,root,root,0755)
#%{prefix}

#%changelog
