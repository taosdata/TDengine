import datetime


tempfile = 'temp'

thisday = datetime.date.today()

errcount = 0
notecount = 0
warncount = 0
taosdefcount = 0
taosldlucount = 0
infd = open(tempfile,'r')
errfd = open('error.%s' %(thisday), 'w')
notefd = open('note.%s' %(thisday), 'w')
warnfd = open('warn.%s' %(thisday), 'w')
taosfd = open('taosdef.%s' %(thisday), 'w')
taosfd.write('Rule:\n')
taosfd.write('1) not use long, long long, unsigned long \n')
taosfd.write('2) defined variable or function not starts with _\n')
taosfd.write('==================================================\n')
recordfd = open('record_check', 'a+')

line = infd.readline()
while(line):
  if ((line.find('.c') > 0) or (line.find('.h') > 0)):
    flag = (line.find('unknown argument') > 0) or (line.find('deps') > 0) or (line.find('tests') > 0)
    if (~flag):
      if (line.find('error:') > 0):
        errcount += 1
        errfd.write(line)
        errfd.write(infd.readline())
        errfd.write(infd.readline())
      elif (line.find('note:') > 0):
        notecount += 1
        notefd.write(line)
        notefd.write(infd.readline())
        notefd.write(infd.readline())
      elif (line.find('warning:') > 0):
        warncount += 1
        warnfd.write(line)
        warnfd.write(infd.readline())
        warnfd.write(infd.readline()) 
  line = infd.readline()
infd.close()

infd = open('taostemp','r')
line = infd.readline()
while(line):
  if ((line.find('.c') > 0) or (line.find('.h') > 0)):
    flag = (line.find('unknown argument') > 0) or (line.find('deps') > 0) or (line.find('tests') > 0)
    if (~flag):
      if (line.find('error:') > 0):
        #taosdefcount += 1
        taosfd.write(line)
        taosfd.write(infd.readline())
        taosfd.write(infd.readline())
      elif (line.find('note:') > 0):
        #taosdefcount += 1
        taosfd.write(line)
        taosfd.write(infd.readline())
        taosfd.write(infd.readline())
      elif (line.find('warning:') > 0):
        taosdefcount += 1
        taosfd.write(line)
        taosfd.write(infd.readline())
        taosfd.write(infd.readline()) 
  line = infd.readline()
infd.close()

infd = open('taos_no_ldlu.%s'%(datetime.date.today()), 'r')
for line in infd.readlines():
  continue
taosldlucount = int(line.strip('\n'))
infd.close()

recordfd.write('%s   %d     %d      %d          %d          %d\n' \
    %(datetime.date.today(), errcount, warncount, notecount, taosdefcount, taosldlucount))


errfd.close()
notefd.close()
warnfd.close()
taosfd.close()
recordfd.close()

