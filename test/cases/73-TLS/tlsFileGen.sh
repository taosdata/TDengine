openssl req -newkey rsa:2048 -nodes -keyout /tmp/ca.key -x509 -days 365 -out /tmp/ca.crt -subj "/CN=MyCA"

# 生成服务器私钥和证书签名请求(CSR)

openssl genrsa -out /tmp/server.key 2048
openssl req -new -key /tmp/server.key -out /tmp/server.csr -subj "/CN=localhost" # CN通常设为服务器域名或IP

# 用CA证书签发服务器证书
openssl x509 -req -in /tmp/server.csr -CA /tmp/ca.crt -CAkey /tmp/ca.key -CAcreateserial -out /tmp/server.crt -days 365

# 生成客户端私钥和证书签名请求(CSR)
openssl genrsa -out /tmp/client.key 2048
openssl req -new -key /tmp/client.key -out /tmp/client.csr -subj "/CN=Client"

# 用CA证书签发客户端证书
openssl x509 -req -in /tmp/client.csr -CA /tmp/ca.crt -CAkey /tmp/ca.key -CAcreateserial -out /tmp/client.crt -days 365