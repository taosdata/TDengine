import sys
## eg: python3 gen_taosadapter_url.py 172.26.10.87 10000:10050

#TaosadapterIp = "172.26.10.87"
TaosadapterIp = sys.argv[1]
#TaosadapterPort = "10000:10050"
TaosadapterPort = sys.argv[2]
start_port = int(TaosadapterPort.split(":")[0])
end_port = int(TaosadapterPort.split(":")[1])
urls_list = []
for port in range(start_port, end_port+1):
	urls_list.append(f"http://{TaosadapterIp}:{port}")
print(urls_list)
	

