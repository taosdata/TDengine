pipeline {
  agent none
  environment{
      WK = '/var/lib/jenkins/workspace/TDinternal'
      WKC= '/var/lib/jenkins/workspace/TDinternal/community'
  }

  stages {
      stage('pre build'){
        agent{label 'master'}
        when{ changeset "develop"}
        steps{
          sh '''
              
              
              echo "check OK!"
              '''
        }
      }
      stage('Parallel test stage') {
      parallel {
        stage('pytest') {
          when{ changeset "develop"}
          agent{label 'master'}
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
            ./test-all.sh pytest
            date'''
          }
        }
        stage('test_b1') {
          when{ changeset "develop"}
          agent{label '184'}
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
          when{ changeset "develop"}
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
          when{ changeset "develop"}
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
            cmake .. > /dev/null
            make > /dev/null
            cd ${WKC}/tests/pytest
            ./valgrind-test.sh 2>&1 > mem-error-out.log
            ./handle_val_log.sh
          
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