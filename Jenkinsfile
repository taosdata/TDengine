pipeline {
  agent none
  environment{
      WK = '/var/lib/jenkins/workspace/TDinternal'
      WKC= '/var/lib/jenkins/workspace/TDinternal/community'
  }
  stages {
    stage('build TDengine') {
      agent{label 'master'}
      steps {
        sh '''
        cd ${WKC}
        git checkout develop
        git pull
        git submodule update
        cd ${WK}
        git checkout develop
        git pull
        export TZ=Asia/Harbin
        date
        rm -rf ${WK}/debug
        mkdir debug
        cd debug
        #cmake .. > /dev/null
        #make > /dev/null
        '''
      }
    }

    stage('Parallel test stage') {
      parallel {
        stage('test_b1') {
          agent{label '184'}
          steps {
            sh '''
            date
            cd ${WKC}
            git checkout develop
            git pull
            git submodule update
            cd ${WK}
            git checkout develop
            git pull
            export TZ=Asia/Harbin
            date
            rm -rf ${WK}/debug
            mkdir debug
            cd debug
            cmake .. > /dev/null
            make > /dev/null
            cd ${WKC}/tests
            #./test-all.sh smoke
            ./test-all.sh b1
            date'''
          }
        }

        stage('test_crash_gen') {
          agent{label "185"}
          steps {
            sh '''
            date
            cd ${WKC}
            git checkout develop
            git pull
            git submodule update
            cd ${WK}
            git checkout develop
            git pull
            export TZ=Asia/Harbin
            date
            rm -rf ${WK}/debug
            mkdir debug
            cd debug
            cmake .. > /dev/null
            make > /dev/null
            cd ${WKC}/tests/pytest
            ./crash_gen.sh -a -p -t 4 -s 2000
            date
            cd ${WKC}/tests
            ./test-all.sh b2
            date
            '''
          }
        }

        stage('test_valgrind') {
          agent{label "186"}
          steps {
            sh '''
            date
            cd ${WKC}
            git checkout develop
            git pull
            git submodule update
            cd ${WK}
            git checkout develop
            git pull
            export TZ=Asia/Harbin
            date
            rm -rf ${WK}/debug
            mkdir debug
            cd debug
            cmake .. > /dev/null
            make > /dev/null
            cd ${WKC}/tests/pytest
            ./start_valgrind.sh 2>&1 > mem-error-out.log
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
            done
            date
            cd ${WKC}/tests
            ./test-all.sh b3
            date'''
          }
        }

      }
    }

  }
}
