## filter a complete process for one sql query

import os;
import sys;

######################## clean last temp file ####################
temp_files = ["taos","116taos","200taosd","tsc.txt","tsc_mnd_rpc.txt","tsc_dnd_rpc.txt","tmp.txt"];
for file in temp_files:
  if (os.path.isfile(file)):
    os.system("rm " +file);
os.system("rm tsc*.txt");
os.system("rm dnd*.txt");
os.system("rm mnd*.txt");
os.system("rm rpc*.txt");

####################### read err message and log directory #################
# logdir = input("input log directory (quote by \"\"): \n");
logdir = "/home/ubuntu/fpan/cluster_test/script/py";
print("logdir is " + logdir +'\n');
#sqlcmd = input("input failed sql query (quote by \"\"): \n");
sqlcmd = "create table";
print("SQL: " + sqlcmd + '\n');


######################## read time from one row log and sort log files by time ############
def get_time(line, dt):
  t = dt;
  idxs = line.find(" ");
  idxe = line.find(" ", idxs+1);
  tmp  = line[idxs+1:idxe];
  ttmp = tmp.split(":");
  return t+float(ttmp[2])+int(ttmp[1])*60+int(ttmp[0])*3600;


def get_sorted_logfiles(prefix):
  file_start_time = [];
  for i in ["0","1"]:
    fname = prefix + i;
    try:
      fd = open(fname, 'r');
      line = fd.readline();
      while (line):
        idx = line.find(":");
        if idx > 0: break;
        line = fd.readline();
      file_start_time.append(get_time(line,0));
    except IOError:
      if i == "1":
        return [prefix+"0"];
      else :
        print("log file " + fname + "not exist!\n");
        return [];
    else:
      fd.close();
  if file_start_time[0] < file_start_time[1]:
    return [prefix+"0",fname];
  else:
    return [fname, prefix+"0"];


######################## analyze one line #################################################################
def analyze_one_line(line):
  res = [];
  tmp = line.split(" ");
  if len(tmp) > 4:
    res.append(tmp[2]);
    res.append(tmp[3]); #threadId, module,2
    # RPC module
    if tmp[3] == "RPC":
      res.append(tmp[4]); #threadId, module, sender-receiver,3
      idxs = line.find("pConn");
      if idxs > 0:
        idxe = line.find(" ", idxs);
        if idxe > 0:
          res.append(line[idxs:idxe]);
        else:
          res.append(line[idxs:-1]); #threadId, module, sender-receiver, pConn,4
        idxs = line.find("source");
        if idxs > 0:
          idxe = line.find(" ", idxs);
          res.append(line[idxs:idxe]);
          idxs = line.find("tranId");
          idxe = line.find(" ", idxs);
          res.append(line[idxs:idxe]);
          #threadId, module, sender2receiver, pConn, source, tranId,6
          idxs = line.find("is sent to ");
          if idxs > 0:
            idxe = line.find(":",idxs);
            recv_ip = line[idxs+11:idxe];
            tmp = recv_ip.split(".");
            recv_ip = tmp[-1];
            res.append(recv_ip); #threadId, module, sender2receiver, pConn, source, tranId, recv_ip,7
    # TSC module
    elif tmp[3] == "TSC":
      idxs = line.find("TSC ");
      idxe = line.find(" ", idxs+4);
      res.append(line[idxs+4:idxe]);
      #threadId, module, TSCobj,3
  return res;


######################## find the server send back message and related operation ############################
def mnd_dnd_log(fname, irow, pConn, source, tranId, mnd_ip, dnd_ip):
  shcmd = "grep -a \"" + pConn[6:] + "\" " + fname + " > tmpmnd_dnd.txt";
  tmp = os.system(shcmd);
  if tmp == 0:
    fd = open("tmpmnd_dnd.txt", "r");
    irow = 0; 
    irow_start = 0;
    irow_end = 0;
    rpc_begin = False;
    line = fd.readline();
    while (line):
      irow += 1;
      idxs = line.find("tranId:");
      if (idxs > 0):
        idxe = line.find(" ", idxs);
        if (~rpc_begin):
          if (line[idxs:idxe] == tranId): 
            rpc_begin = True;
            irow_start = irow;
        else:
          if (line[idxs:idxe] != tranId):
            irow_end = irow;
            break;
      line = fd.readline();
    fd.close();
    if (irow_end == 0): irow_end = irow;
    shcmd = "head -n " + str(irow_end) + " tmpmnd_dnd.txt | tail -n +" + str(irow_start) \
      + "> mnd" + mnd_ip + "_dnd" + dnd_ip + ".txt";
    tmp = os.system(shcmd);
    if (tmp == 0):
      os.system("rm -rf tmpmnd_dnd.txt");
      fd = open("mnd" + mnd_ip + "_dnd" + dnd_ip + ".txt", "r");
      line = fd.readline();
      while (line):
        analy_res = analyze_one_line(line);
        if (len(analy_res) > 6): #threadId, module, sender2receiver, pConn, source, tranId, recv_ip,7
          dnd_mnd_log(analy_res[6]+"taosd", 0, analy_res[4], analy_res[5], mnd_ip, analy_res[6]);
          break;
        line = fd.readline();
    else:
      print(shcmd + "failed");
  else:
    print(shcmd + " failed!");

  
def dnd_mnd_log(fname, irow, source, tranId, mnd_ip, dnd_ip):
  shcmd = "grep -a \"" + tranId + "\" " + fname + "| grep -a \"" + source + "\"" \
    + " > tmpdnd_mnd.txt";
  tmp = os.system(shcmd);
  pConn = "";
  if tmp == 0:
    fd = open("tmpdnd_mnd.txt", "r");
    line = fd.readline();
    fd.close();
    if (line):
      analy_res = analyze_one_line(line);
      if (len(analy_res) > 6):#threadId, module, sender2receiver, pConn, source, tranId, recv_ip,7
        pConn = analy_res[3];
    else:
      print("cannot find dnd to mnd response with " +tranId);
  if len(pConn) > 0:
    os.system("rm -rf tmpdnd_mnd.txt");
    shcmd = "grep -a \"" + pConn[6:] + "\" " + fname + " > tmpdnd_mnd.txt";
    tmp = os.system(shcmd);
    if tmp == 0:
      fd = open("tmpdnd_mnd.txt", "r");
      irow = 0; 
      irow_start = 0;
      irow_end = 0;
      rpc_begin = False;
      line = fd.readline();
      while (line):
        irow += 1;
        idxs = line.find("tranId:");
        if (idxs > 0):
          idxe = line.find(" ", idxs);
          if (~rpc_begin):
            if (line[idxs:idxe] == tranId): 
              rpc_begin = True;
              irow_start = irow;
          else:
            if (line[idxs:idxe] != tranId):
              irow_end = irow;
              break;
        line = fd.readline();
      fd.close();
      if (irow_end == 0): irow_end = irow;
      shcmd = "head -n " + str(irow_end) + " tmpdnd_mnd.txt | tail -n +" + str(irow_start) \
        + "> dnd" + mnd_ip + "_mnd" + dnd_ip + ".txt";
      tmp = os.system(shcmd);
      if (tmp == 0):
        os.system("rm -rf tmpmnd_dnd.txt");
      else:
        print(shcmd + "failed");
    else:
      print(shcmd + " failed!");


def vnode_client_log(fname, source, tranId, server_ip):
  source_id = source[7:];
  fd = open(fname, "r");
  line = fd.readline();
  while (line):
    if (line.find(tranId) > 0):
      if (line.find(source_id) > 0): break;
    line = fd.readline();
  analy_res = analyze_one_line(line);
  pConn = analy_res[3];
  # begin to anaylze server log and find related dnd log
  os.system("rm dnd"+server_ip+"-tsc-rpc.txt");
  dnd_tsc_rpc_fd = open("dnd"+server_ip+"-tsc-rpc.txt", "w");
  dnd_fd         = open("dnd"+server_ip+".txt","w");
  dnd_thread     = "";
  rpc_end = False;
  while (line):
    if (len(analy_res) == 0): 
      line = fd.readlind();
      irow += 1;
      continue;
    if ((analy_res[1] == "RPC") & ~rpc_end):
      if ((analy_res[2] == "DND-shell") & (len(analy_res) > 3)):
        if analy_res[3] == pConn: 
          if (len(analy_res) > 5):
            if (analy_res[5] != tranId): 
              rpc_end = True;
            else:
              dnd_tsc_rpc_fd.write(line);
          else: 
            dnd_tsc_rpc_fd.write(line);
    elif analy_res[1] == "DND":
      if (len(dnd_thread) > 0):
        if (analy_res[0] == dnd_thread): dnd_fd.write(line);
      else:
        dnd_thread = analy_res[0];
        dnd_fd.write(line);
    line = fd.readline();
    analy_res = analyze_one_line(line);
  fd.close();
  dnd_tsc_rpc_fd.close();
  dnd_fd.close();


def mnd_client_log(fname, source, tranId, server_ip):
  source_id = source[7:]; 
  fd = open(fname, "r");
  irow = 0;
  line = fd.readline();
  while (line):
    irow += 1;
    if (line.find(tranId) > 0):
      if (line.find(source_id) > 0): break;
    line = fd.readline();
  analy_res = analyze_one_line(line);
  pConn = analy_res[3];
  # begin to anaylze server log and find related mnd log
  if (os.path.isfile("mnd"+server_ip+"_tsc_rpc.txt")):
    os.system("rm mnd"+server_ip+"_tsc_rpc.txt");
  mnd_tsc_rpc_fd = open("mnd"+server_ip+"_tsc_rpc.txt", "w");
  mnd_fd         = open("mnd"+server_ip+".txt","w");
  mnd_thread     = "";
  rpc_end = False;
  rpc_mnd_dnd_end = False;
  while (line):
    if (len(analy_res) == 0): 
      line = fd.readline();
      irow += 1;
      continue;
    if ((analy_res[1] == "RPC") & ~rpc_end):
      if ((analy_res[2] == "MND-shell") & (len(analy_res) > 3)):
        if analy_res[3] == pConn: 
          if (len(analy_res) > 5):
            if (analy_res[5] != tranId): 
              rpc_end = True;
            else:
              mnd_tsc_rpc_fd.write(line);
          else: 
            mnd_tsc_rpc_fd.write(line);
      elif ((analy_res[2] == "MND-dnode") & ~rpc_mnd_dnd_end): 
        if (len(analy_res) > 6): #threadId, module, sender2receiver, pConn, source, tranId, recv_ip,7
          rpc_mnd_dnd_end = True;
          if (analy_res[6] == server_ip):
            mnd_dnd_log(fname, irow, analy_res[3], analy_res[4], analy_res[5], server_ip, analy_res[6]);
            #dnd_mnd_log(fname, irow, analy_res[4], analy_res[5], server_ip, analy_res[6]);
          else:
            mnd_dnd_log(fname, irow, analy_res[3], analy_res[4], analy_res[5], server_ip, analy_res[6]);
            #dnd_mnd_log(analy_res[6]+"taosd", 0, analy_res[4], analy_res[5], server_ip, analy_res[6]);
    elif analy_res[1] == "MND":
      if (len(mnd_thread) > 0):
        if (analy_res[0] == mnd_thread): mnd_fd.write(line);
      else:
        mnd_thread = analy_res[0];
        mnd_fd.write(line);
    line = fd.readline();
    analy_res = analyze_one_line(line);
    irow += 1;
  fd.close();
  mnd_tsc_rpc_fd.close();
  mnd_fd.close();




######################## sort all log files and combine them into one file ################################
client_log = "taos";
dnode_list = [];
while(1):
  one_dnode = input("input last part of the IP for the dnode, (quote by \"\") stop by entering \"e\": \n");
  if one_dnode == "e": break;
  dnode_list.append(one_dnode);

print("Show dnodes:");
for i in range(len(dnode_list)):
  print(dnode_list[i]);

client_log_files_prefix = logdir + "/taoslog0.";
client_log_files = get_sorted_logfiles(client_log_files_prefix);
if len(client_log_files) < 1: system.exit(1);
for i in range(len(client_log_files)):
  shcmd = "cat " + client_log_files[i] + " >>taos";
  os.system(shcmd);
for i in range(len(dnode_list)):
  server_log_dir = logdir + "/" + dnode_list[i];
  server_log_files_prefix = server_log_dir + "/taosdlog.";
  server_log_files = get_sorted_logfiles(server_log_files_prefix);
  if len(server_log_files) < 1: sys.exit(1);
  for j in range(len(server_log_files)):
    shcmd = "cat " + server_log_files[j] + ">>" + dnode_list[i] +"taosd";
    os.system(shcmd);



######################## start from client TSC-mgmt RPC communication ###############################
client_irow = 0;
server_irow = [0]*len(dnode_list);
client_fd = open("taos", "r");

line = client_fd.readline();
# find the start of sql command execution
while (line):
  client_irow += 1;
  idx = line.find(sqlcmd);
  if idx > 0: break;
  line = client_fd.readline();

analy_res = analyze_one_line(line);
tscobj = analy_res[2];
rpc_begin = False;
tsc_fd = open("tsc.txt", "w");
tsc_fd.write(line);
tsc_mnd_rpc_fd = open("tsc_mnd_rpc.txt", "w");
tsc_mnd_pconn = "";
tsc_dnd_rpc_fd = open("tsc_dnd_rpc.txt", "w");
tsc_dnd_pconn = "";

line = client_fd.readline();
# analyze the client log until getting the SQL result
# if client sent msg to server, find corresponding msg in server
tsc_mnd_rpc_end = False;
tsc_dnd_rpc_end = False;
while(line):
  client_irow += 1;
  analy_res = analyze_one_line(line);
  if len(analy_res) > 2:
    if ((analy_res[1] == "TSC") & (analy_res[2] == tscobj)):
      tsc_fd.write(line);
      if ~rpc_begin:
        idx = line.find("pConn");
        if idx > 0:
          rpc_begin = True;
    elif ((analy_res[1] == "RPC") & rpc_begin):
      if (analy_res[2] == "TSC-mgmt"):
        if len(tsc_mnd_pconn) > 0:
          if len(analy_res) > 3: #threadId, module, sender2receiver, pConn, source, tranId
            if (analy_res[3] == tsc_mnd_pconn): tsc_mnd_rpc_fd.write(line);
        else:
          if len(analy_res) > 3:
            tsc_mnd_rpc_fd.write(line);
            tsc_mnd_pconn = analy_res[3];
            if ((len(analy_res) > 6) & ~tsc_mnd_rpc_end):
              server_ip = analy_res[6];
              mnd_client_log(server_ip+"taosd", analy_res[4], analy_res[5], server_ip);
              tsc_mnd_rpc_end = True;
      elif (analy_res[2] == "TSC-vnode"):
        if len(tsc_dnd_pconn) > 0:
          if len(analy_res) > 3: #threadId, module, sender2receiver, pConn, source, tranId
            if (analy_res[3] == tsc_dnd_pconn): tsc_dnd_rpc_fd.write(line);
        else:
          if len(analy_res) > 3:
            tsc_dnd_rpc_fd.write(line);
            tsc_dnd_pconn = analy_res[3];
            if ((len(analy_res) > 6) & ~tsc_dnd_rpc_end):
              server_ip = analy_res[6];
              vnode_client_log(server_ip+"taosd", analy_res[4], analy_res[5], server_ip);
              tsc_dnd_rpc_end = True;
  line = client_fd.readline();
  idx = line.find("SQL result");
  if idx > 0: 
    tsc_fd.write(line);
    break;


tsc_fd.close();
tsc_mnd_rpc_fd.close();
tsc_dnd_rpc_fd.close();
client_fd.close();
