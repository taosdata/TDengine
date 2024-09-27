Summary: SASL API implementation
Name: sasl
Version: 2.0.1
Release: 1
Copyright: CMU
Group: Libraries
Source: ftp.andrew.cmu.edu:/pub/cyrus-mail/cyrus-sasl-2.0.1-ALPHA.tar.gz
Packager: Rob Earhart <earhart@cmu.edu>
Requires: gdbm

%description
This is an implemention of the SASL API, useful for adding
authentication, authorization, and security to network protocols.  The
SASL protocol itself is documented in rfc2222; the API standard is a
work in progress.

%package devel
%summary: SASL development headers and examples

%description devel
This includes the header files and documentation needed to develop
applications which use SASL.

%package plug-anonymous
%summary: SASL ANONYMOUS mechanism plugin

%description plug-anonymous
This plugin implements the SASL ANONYMOUS mechanism,
used for anonymous authentication.

%package plug-crammd5
%summary: SASL CRAM-MD5 mechanism plugin

%description plug-crammd5
This plugin implements the SASL CRAM-MD5 mechanism.
CRAM-MD5 is the mandatory-to-implement authentication mechanism for a
number of protocols; it uses MD5 with a challenge/response system to
authenticate the user.

%package plug-digestmd5
%summary: SASL DIGEST-MD5 mechanism plugin

%description plug-digestmd5
This plugin implements the latest draft of the SASL DIGEST-MD5
mechanism.  Although not yet finalized, this is likely to become the
new mandatory-to-implement authentication system in all new protocols.
It's based on the digest md5 authentication system designed for HTTP.

%package plug-kerberos4
%summary: SASL KERBEROS_V4 mechanism plugin

%description plug-kerberos4
This plugin implements the SASL KERBEROS_V4 mechanism, allowing
authentication via kerberos version four.

%package plug-plain
%summary: SASL PLAIN mechanism plugin

%description plug-plain
This plugin implements the SASL PLAIN mechanism.  Although insecure,
PLAIN is useful for transitioning to new security mechanisms, as this
is the only mechanism which gives the server a copy of the user's
password.

%package plug-scram
%summary: SASL SCRAM-SHA-1/SCRAM-SHA-2 mechanism plugin

%description plug-scram
This plugin implements the SASL SCRAM-SHA-1/SCRAM-SHA-2 mechanism.

%prep
%setup

%build
./configure --prefix=/usr --disable-java
make

%install
make install

%post
if test $RPM_INSTALL_PREFIX/lib/sasl != /usr/lib/sasl; then
  ln -s $RPM_INSTALL_PREFIX/lib/sasl /usr/lib/sasl
fi

%postun
if test -L /usr/lib/sasl; then
  rm /usr/lib/sasl
fi

%files
%doc README COPYING ChangeLog NEWS AUTHORS
/usr/lib/libsasl.so.5.0.0
/usr/sbin/saslpasswd
/usr/man/man8/saslpasswd.8

%files devel
%doc doc/rfc2222.txt sample/sample-client.c sample/sample-server.c testing.txt
/usr/lib/libsasl.la
/usr/include/sasl.h
/usr/include/saslplug.h
/usr/include/saslutil.h
/usr/include/md5global.h
/usr/include/md5.h
/usr/include/hmac-md5.h

%files plug-anonymous
%doc doc/draft-newman-sasl-anon-00.txt
/usr/lib/sasl/libanonymous.so.1.0.2
/usr/lib/sasl/libanonymous.so

%files plug-crammd5
%doc doc/rfc1321.txt doc/rfc2095.txt doc/rfc2104.txt
/usr/lib/sasl/libcrammd5.so.1.0.1
/usr/lib/sasl/libcrammd5.so

%files plug-digestmd5
%doc doc/draft-leach-digest-sasl-01.txt 
/usr/lib/sasl/libdigestmd5.so.0.0.1
/usr/lib/sasl/libdigestmd5.so

%files plug-kerberos4
/usr/lib/sasl/libkerberos4.so.1.0.2
/usr/lib/sasl/libkerberos4.so

%files plug-plain
/usr/lib/sasl/libplain.so.1.0.1
/usr/lib/sasl/libplain.so
