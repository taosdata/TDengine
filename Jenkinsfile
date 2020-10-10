pipeline {
  agent any
  stages {
    stage('build TDengine') {
      steps {
        sh '''cd ${WORKSPACE}
export TZ=Asia/Harbin
date
rm -rf ${WORKSPACE}/debug
mkdir debug
cd debug
cmake .. > /dev/null
make > /dev/null
cd ${WORKSPACE}/debug'''
      }
    }

    stage('test_tsim') {
      parallel {
        stage('test') {
          steps {
            sh '''cd ${WORKSPACE}/tests
#./test-all.sh smoke
sudo ./test-all.sh full'''
          }
        }

        stage('test_crash_gen') {
          steps {
            sh '''cd ${WORKSPACE}/tests/pytest
sudo ./crash_gen.sh -a -p -t 4 -s 2000'''
          }
        }

        stage('test_valgrind') {
          steps {
            sh '''cd ${WORKSPACE}/tests/pytest
sudo ./valgrind-test.sh 2>&1 > mem-error-out.log
grep \'start to execute\\|ERROR SUMMARY\' mem-error-out.log|grep -v \'grep\'|uniq|tee uniq-mem-error-out.log

for memError in `grep \'ERROR SUMMARY\' uniq-mem-error-out.log | awk \'{print $4}\'`
do
  if [ -n "$memError" ]; then
    if [ "$memError" -gt 12 ]; then
      echo -e "${RED} ## Memory errors number valgrind reports is $memError.\\
               More than our threshold! ## ${NC}"
      travis_terminate $memError
    fi
  fi
done

grep \'start to execute\\|definitely lost:\' mem-error-out.log|grep -v \'grep\'|uniq|tee uniq-definitely-lost-out.log
for defiMemError in `grep \'definitely lost:\' uniq-definitely-lost-out.log | awk \'{print $7}\'`
do
  if [ -n "$defiMemError" ]; then
    if [ "$defiMemError" -gt 13 ]; then
      echo -e "${RED} ## Memory errors number valgrind reports \\
               Definitely lost is $defiMemError. More than our threshold! ## ${NC}"
      travis_terminate $defiMemError
    fi
  fi
done'''
          }
        }

      }
    }

  }
}