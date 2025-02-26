%define homepath         /usr/local/taos
%define userlocalpath    /usr/local
%define cfg_install_dir  /etc/taos
%define __strip /bin/true

Name:		taostools
Version:	%{_version}
Release:	3%{?dist}
Summary:	from taosdata
Group:	  Application/Database
License:	AGPL
URL:		  www.taosdata.com
AutoReqProv: no

#BuildRoot:  %_topdir/BUILDROOT
BuildRoot:   %{_tmppath}/%{name}-%{version}-%{release}-root

#Prefix: /usr/local/taos

#BuildRequires:
Requires: tdengine

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

# create install path, and cp file
mkdir -p %{buildroot}%{homepath}/bin

cp %{_compiledir}/build/bin/taosdump                %{buildroot}%{homepath}/bin
#cp %{_compiledir}/build/bin/taosBenchmark           %{buildroot}%{homepath}/bin
cp %{_compiledir}/build/bin/TDinsight.sh            %{buildroot}%{homepath}/bin

#if [ -f %{_compiledir}/build/lib/libavro.so.23.0.0 ]; then
#    mkdir -p %{buildroot}%{userlocalpath}/lib
#    cp %{_compiledir}/build/lib/libavro.so.23.0.0 %{buildroot}%{userlocalpath}/lib
#    ln -sf libavro.so.23.0.0 %{buildroot}%{userlocalpath}/lib/libavro.so.23
#    ln -sf libavro.so.23 %{buildroot}%{userlocalpath}/lib/libavro.so
#fi

#if [ -f %{_compiledir}/build/lib/libavro.a ]; then
#    cp %{_compiledir}/build/lib/libavro.a %{buildroot}%{userlocalpath}/lib
#fi

#Scripts executed before installation
%pre
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

#Scripts executed after installation
%post
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

${csudo}mkdir -p /usr/local/bin || :
${csudo}ln -sf /usr/local/taos/bin/taosdump          /usr/local/bin/taosdump

if [[ -f /usr/local/taos/bin/taosBenchmark ]]; then
    ${csudo}ln -sf /usr/local/taos/bin/taosBenchmark     /usr/local/bin/taosBenchmark
    ${csudo}ln -sf /usr/local/taos/bin/taosBenchmark     /usr/local/bin/taosdemo
fi

if [[ -d /usr/local/lib64 ]]; then
    ${csudo}ln -sf /usr/local/lib/libavro.so.23.0.0 /usr/local/lib64/libavro.so.23.0.0 || :
    ${csudo}ln -sf /usr/local/lib64/libavro.so.23.0.0 /usr/local/lib64/libavro.so.23 || :
    ${csudo}ln -sf /usr/local/lib64/libavro.so.23 /usr/local/lib64/libavro.so || :

    if [ -d /etc/ld.so.conf.d ]; then
        ${csudo}echo "/usr/local/lib64" > /etc/ld.so.conf.d/libavro.conf
        ${csudo}ldconfig
    else
        echo "/etc/ld.so.conf.d not found!"
    fi
else
    if [ -d /etc/ld.so.conf.d ]; then
        ${csudo}echo "/usr/local/lib" > /etc/ld.so.conf.d/libavro.conf
        ${csudo}ldconfig
    else
        echo "/etc/ld.so.conf.d not found!"
    fi
fi

# Scripts executed before uninstall
%preun
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi
# only remove package to call preun.sh, not but update(2)
${csudo}rm -f /usr/local/bin/taosdump      || :
#${csudo}rm -f /usr/local/bin/taosBenchmark || :
#${csudo}rm -f /usr/local/bin/taosdemo      || :

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

echo -e "${GREEN}taosTools is removed successfully!${NC}"

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
