# -*- coding: utf-8 -*- 
import sys
import os

modules = ['import_merge',\
           'multi_import_merge']
cluster_modules = ['cluster',\
                   'cluster_mgmt',\
                   'import_cluster']             

rootDir = os.getcwd()
outfile = 'testlist.sh'
shcmd = 'rm -rf %s' %outfile
if os.system(shcmd) != 0:
  print '%s: failed' %shcmd
  sys.exit(1)
outfd = open(outfile, "w")

if __name__=="__main__":
  for module in modules:
    moduleDir = os.path.realpath(rootDir+"/"+module)
    for file in os.listdir(moduleDir):
      if file.startswith("_"): continue
      if file.endswith(".pyc"): continue
      #shcmd = 'sudo python2 %s/test.py -f %s/%s/%s\n' %(rootDir, rootDir, module, file)
      shcmd = 'sudo python2 test.py -f %s/%s\n' %(module, file)
      outfd.write(shcmd)

  for module in cluster_modules:
    moduleDir = os.path.realpath(rootDir+"/"+module)
    for file in os.listdir(moduleDir):
      if file.startswith("_"): continue
      if file.endswith(".pyc"): continue
      #shcmd = 'sudo python2 %s/test.py -f %s/%s/%s -c\n' %(rootDir, rootDir, module, file)
      shcmd = 'sudo python2 test.py -f %s/%s -c\n' %(module, file)
      outfd.write(shcmd)
  
  outfd.close()
  shcmd = 'sudo chmod 755 %s' %outfile
  if os.system(shcmd) != 0:
    print '%s: failed' %shcmd
    sys.exit(1)
