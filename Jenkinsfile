pipeline {
  agent none
  environment{
      WK = '/var/lib/jenkins/workspace/TDinternal'
      WKC= '/var/lib/jenkins/workspace/TDinternal/community'
  }

  stages {
      stage('Parallel test stage') {
      parallel {
        stage('pytest') {
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
            ./test-all.sh pytest
            date'''
          }
        }
        stage('test_b1') {
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
            '''
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WKC}/tests/pytest
                ./crash_gen.sh -a -p -t 4 -s 2000
                '''
            }
            sh '''
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
       stage('connector'){
         agent{label "release"}
         steps{
            sh'''
            cd ${WORKSPACE}
            git checkout develop
            cd tests/gotest
            bash batchtest.sh
            cd ${WORKSPACE}/tests/examples/JDBC/JDBCDemo/
            mvn clean package assembly:single >/dev/null 
            java -jar target/jdbcChecker-SNAPSHOT-jar-with-dependencies.jar -host 127.0.0.1
            cd ${WORKSPACE}/tests/examples/python/PYTHONConnectorChecker
            python3 PythonChecker.py
            '''
         }
       }

      }
    }

  }
  
}