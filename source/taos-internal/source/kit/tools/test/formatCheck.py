'''
author: Fang Pan
date:   2019-12-21
object: find '%lu, %ld' format in print and output warning
'''


import sys
import getopt
import datetime



outfd = open('taos_no_ldlu.%s'%(datetime.date.today()), 'w')
outfd.write('Check: no %ld or %lu\n')
outfd.write('===============================================\n')
opts, args = getopt.getopt(sys.argv[1:], "hp:")
for op, value in opts:
  if op == '-p':
    checkFile = value
  elif op =='-h':
    print('-p: path for compile database')
    sys.exit(0)
  else:
    print("Error: incorrect arguments!")
    sys.exit(1)


checkfiles = []
try:
  checkfd = open('%s/compile_commands.json' %(checkFile), 'r')
  for line in checkfd.readlines():
    if (line.find('\"file\"') >= 0):
      tmp = line.split(':')
      fileToCheck = tmp[1].strip()
      checkfiles.append(eval(fileToCheck.strip('\n')))
except:
  print('Error: file for compile database not found')
  sys.exit(1)
checkfd.close()

errcnt = 0
for fileToCheck in checkfiles:
  try:
    infd = open(fileToCheck, 'r')
    rowrecord = []
    irow = 0
    for line in infd.readlines():
      irow += 1
      if (line.find('//') > 0): continue
      idx = max(line.find('%lu'), line.find('%ld'))
      if (idx >=0):
        errcnt += 1
        outfd.write('%s: %d:%d\n' %(fileToCheck,irow,idx))
        outfd.write(line)
    infd.close()
  except:
    print('Error: %s not found' %fileToCheck)
    break
    #sys.exit(1) 

outfd.write("Suggestion: \n")
outfd.write('(1) replace by %lld or %llu, but there is warning in the compilation\n')
outfd.write('(2) replace by \"PRId64\" or \"PRIu64\"\n')
outfd.write('%d\n' %errcnt)
outfd.close()