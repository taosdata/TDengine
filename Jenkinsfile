import hudson.model.Result
import jenkins.model.CauseOfInterruption
properties([pipelineTriggers([githubPush()])])
node {
    git url: 'https://github.com/taosdata/TDengine.git'
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

    build.doKill()    //doTerm(),doKill(),doTerm()
  }
}
//abort previous build
abortPreviousBuilds()
def abort_previous(){
  def buildNumber = env.BUILD_NUMBER as int
  if (buildNumber > 1) milestone(buildNumber - 1)
  milestone(buildNumber)
}
def pre_test(){
    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                sudo rmtaos
                '''
    }
    sh '''
    
    cd ${WKC}
    git checkout develop
    git pull
    git fetch
    git checkout ${CHANGE_BRANCH}
    git pull
    git merge develop
    cd ${WK}
    git reset --hard
    git checkout develop
    git pull
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
        //only build pr
        when {
              changeRequest()
          }
      parallel {
        stage('python_1') {
          agent{label 'p1'}
          steps {
            
            pre_test()
            sh '''
            cd ${WKC}/tests
            ./test-all.sh p1
            date'''
          }
        }
        stage('python_2') {
          agent{label 'p2'}
          steps {
            
            pre_test()
            sh '''
            cd ${WKC}/tests
            ./test-all.sh p2
            date'''
          }
        }
        stage('test_b1') {
          agent{label 'b1'}
          steps {            
            pre_test()
            sh '''
            cd ${WKC}/tests
            ./test-all.sh b1fq
            date'''
          }
        }

        stage('test_crash_gen') {
          agent{label "b2"}
          steps {
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
            ./test-all.sh b2fq
            date
            '''
          }
        }

        stage('test_valgrind') {
          agent{label "b3"}

          steps {
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
            ./test-all.sh b3fq
            date'''
          }
        }
   
        
    }
  }
  }
   
}
