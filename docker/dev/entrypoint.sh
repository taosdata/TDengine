#!/bin/bash
. /opt/rh/devtoolset-11/enable
ln /opt/rh/devtoolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11/libgcc.a /usr/lib64/
ln /opt/rh/devtoolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11/libgcc_s.so /usr/lib64/
ln /opt/rh/devtoolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11/libstdc++.a /usr/lib64/
ln /opt/rh/devtoolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11/libsupc++.a /usr/lib64/
ln /opt/rh/devtoolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11/libstdc++_nonshared.a /usr/lib64/
ln /opt/rh/devtoolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11/libstdc++fs.a /usr/lib64/
ln /opt/rh/devtoolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11/crtendS.o /usr/lib64/
ln /opt/rh/devtoolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11/crtbeginS.o /usr/lib64/

set -x
$@
sccache -s
