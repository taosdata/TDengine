import sys
import os

if __name__ == "__main__":
  file = sys.argv[1]
  #file = '/home/ubuntu/fpan/workspace/report/static_check_report20191204'

  outputFd = open('%s.nodeps' %file, 'w')
  try:
    inputFd = open(file,'r')
    line = inputFd.readline()
    while(line):
      if (line.find('deps') > 0): 
        line = inputFd.readline()
        line = inputFd.readline()
      if ((line.find('.c') > 0) or (line.find('.h') > 0)):
        outputFd.write(line)
        outputFd.write(inputFd.readline())
        outputFd.write(inputFd.readline())
      line = inputFd.readline()
    inputFd.close()
  except:
    print('File %s not found' %file)
    sys.exit(1)
  outputFd.close()
