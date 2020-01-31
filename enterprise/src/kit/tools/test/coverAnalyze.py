import os
import sys
import datetime

if __name__ == "__main__":
  file = sys.argv[1]
  #file = '/home/fang/workspace2/coverage20191204'
  outputCover = {} #coverage:[filename]
  singleCover = 0
  singleFile = ""
  singleLines = 0
  try:
    inputFd = open(file,'r')
    line = inputFd.readline()
    while (line):
      if (line.find('.c') > 0): break
      line = inputFd.readline()
    while (line):
      tmp = line.split()
      singleFile = tmp[0]
      if (len(tmp) > 1):
        singleCover = int(tmp[3].strip('%'))
        singleLines = int(tmp[1])
      else:
        line = inputFd.readline()
        tmp = line.split()
        if (tmp[0].find('A') > 0): break
        singleCover = int(tmp[2].strip('%'))
        singleLines = int(tmp[0])
      if outputCover.has_key(singleCover):
        outputCover[singleCover].append((singleFile,singleLines))
      else:
        outputCover[singleCover] = [(singleFile,singleLines)]
      line = inputFd.readline()
    inputFd.close()
  except:
    print ('%s failed to open' %file)
    sys.exit(1)

  totalLines = 0
  totalCover = 0
  outputFd = open('%s.csv' %file, 'w')
  outputFd.write('Coverage(%), File, FileLines\n')
  for key in outputCover.keys():
    for value in outputCover[key]:
      if (value[0].find('deps') < 0):
        totalLines += value[1]
        totalCover += key*value[1]
      outputFd.write('%d, %s, %d\n' %(key, value[0], value[1]))
  outputFd.write('%f, %s, %d\n' %(totalCover*1.0/totalLines, 'Total', totalLines))
  outputFd.close()

  outRecFd = open('record_coverage','a+')
  if (totalLines > 0):
    outRecFd.write('%s   %f\n' %(datetime.date.today(),totalCover*1.0/totalLines))
  else:
    outRecFd.write('%s   coverage collection failed!\n' %(datetime.date.today()))
  outRecFd.close()
