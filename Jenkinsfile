
properties([pipelineTriggers([githubPush()])])
node {
    git url: 'https://github.com/taosdata/TDengine'
}


// execute this before anything else, including requesting any time on an agent
if (currentBuild.rawBuild.getCauses().toString().contains('BranchIndexingCause')) {
  print "INFO: Build skipped due to trigger being Branch Indexing"
  currentBuild.result = 'success skip' // optional, gives a better hint to the user that it's been skipped, rather than the default which shows it's successful
  return
}
def abortPreviousBuilds() {
  def currentJobName = env.JOB_NAME
  def currentBuildNumber = env.BUILD_NUMBER.toInteger()
  def jobs = Jenkins.instance.getItemByFullName(currentJobName)
  def builds = jobs.getBuilds()

  for (build in builds) {
    if (!build.isBuilding()) {
      continue;
    }

    if (currentBuildNumber == build.getNumber().toInteger()) {
      continue;
    }

    build.doStop()
  }
}
abortPreviousBuilds()
def pre_test(){
    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                sudo rmtaos
                '''
    }
    sh '''
    cd ${WKC}
    rm -rf *
    cd ${WK}
    git reset --hard
    git checkout develop
    git pull
    cd ${WKC}
    rm -rf *
    mv ${WORKSPACE}/* .
    cd ${WK}
    export TZ=Asia/Harbin
    date
    rm -rf ${WK}/debug
    mkdir debug
    cd debug
    cmake .. > /dev/null
    make > /dev/null
    make install > /dev/null
    cd ${WKC}/tests
    '''
    return 1
}
pipeline {
  agent none
  environment{
      WK = '/var/lib/jenkins/workspace/TDinternal'
      WKC= '/var/lib/jenkins/workspace/TDinternal/community'
  }

  stages {
      stage('Parallel test stage') {
      parallel {
        stage('python p1') {
          agent{label 'p1'}
          steps {
            abortPreviousBuilds()
            pre_test()
            sh '''
            cd ${WKC}/tests
            ./test-all.sh p1
            date'''
          }
        }
        stage('test_b1') {
          agent{label 'b1'}
          steps {
            abortPreviousBuilds()
            pre_test()
            sh '''
            cd ${WKC}/tests
            ./test-all.sh b1
            date'''
          }
        }

        stage('test_crash_gen') {
          agent{label "b2"}
          steps {
            abortPreviousBuilds()
            pre_test()
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WKC}/tests/pytest
                ./crash_gen.sh -a -p -t 4 -s 2000
                '''
            }
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WKC}/tests/pytest
                ./handle_crash_gen_val_log.sh
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
          agent{label "b3"}

          steps {
            abortPreviousBuilds()
            pre_test()
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WKC}/tests/pytest
                ./valgrind-test.sh 2>&1 > mem-error-out.log
                ./handle_val_log.sh
                '''
            }           
            sh '''
            date
            cd ${WKC}/tests
            ./test-all.sh b3
            date'''
          }
        }
       stage('python p2'){
         agent{label "p2"}
         steps{
           abortPreviousBuilds()
            pre_test()         
            sh '''
            date
            cd ${WKC}/tests
            ./test-all.sh p2
            date
            '''
         }
       }   
      }
    }
  }
  
}
