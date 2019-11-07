#USE openresty-1.11.2.5


##INSTALL

1. sudo apt-get install libssl-dev
./configure --prefix=/where/you/want/to/install --with-pcre=/where/the/source/pcre-8.41
make;
make install

2. copy all .lua to lualib/resty directory
cp -ri ~/dev/taosdata/thirdparty/openresty/lualib/resty/* ~/opt/lualib/resty/.

3. use ~/dev/taosdata/thirdparty/openresty/nginx/conf/nginx.conf

#RUN
~/opt/nginx/sbin/nginx -c ~/dev/taosdata/thirdparty/openresty/nginx/conf/nginx.conf

