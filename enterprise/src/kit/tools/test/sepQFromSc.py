import sys
import os
import datetime

if __name__ == "__main__":
  file = sys.argv[1]
  #file = '/home/ubuntu/fpan/workspace/report/static_check_report20191204'

  outErrFd = open('%s.err' %file, 'w')
  errCounter = 0
  outWarnFd = open('%s.warn' %file, 'w')
  warnCounter = 0
  outTestFd = open('%s.test' %file, 'w')
  testCounter = 0
  outDepsFd = open('%s.deps' %file, 'w')
  depsCounter = 0
  outOthersFd = open('%s.others' %file, 'w')
  otherCounter = 0
  outCalFd = open('%s.count' %file, 'a')
  try:
    inputFd = open(file,'r')
    line = inputFd.readline()
    while(line):
      if (line.find('deps') > 0): 
        depsCounter += 1
        outDepsFd.write(line)
        outDepsFd.write(inputFd.readline())
        outDepsFd.write(inputFd.readline())
      elif ((line.find('.c') > 0) or (line.find('.h') > 0)):
        if (line.find('tests') > 0):
          testCounter += 1
          outTestFd.write(line)
          outTestFd.write(inputFd.readline())
          outTestFd.write(inputFd.readline())
        elif (line.find('error') > 0):
          errCounter += 1
          outErrFd.write(line)
          outErrFd.write(inputFd.readline())
          outErrFd.write(inputFd.readline())
        elif (line.find('warning') > 0):
          warnCounter += 1
          outWarnFd.write(line)
          outWarnFd.write(inputFd.readline())
          outWarnFd.write(inputFd.readline())
        else:
          otherCounter += 1
          outOthersFd.write(line)
          outOthersFd.write(inputFd.readline())
          outOthersFd.write(inputFd.readline())
      line = inputFd.readline()
    inputFd.close()
  except:
    print('File %s not found' %file)
    sys.exit(1)
  outErrFd.write('There is total %d errors\n' %errCounter)
  outErrFd.close()
  outWarnFd.write('There is total %d warnings\n' %warnCounter)
  outWarnFd.close()
  outTestFd.write('There is total %d questions in test script\n' %testCounter)
  outTestFd.close()
  outDepsFd.write('There is total %d questions in dependencies\n' %depsCounter)
  outDepsFd.close()
  outOthersFd.write('There is total %d qeustions except error/warnings\n' %otherCounter)
  outOthersFd.close()
  outCalFd.write('%s   %d   %d   %d   %d   %d\n' \
    %(datetime.date.today(), errCounter, warnCounter, otherCounter, testCounter, depsCounter))
  outCalFd.close()
