
properties([pipelineTriggers([githubPush()])])
node {
    git url: 'https://github.com/liuyq-617/TDengine.git'
}

// execute this before anything else, including requesting any time on an agent
// if (currentBuild.rawBuild.getCauses().toString().contains('BranchIndexingCause')) {
//   print "INFO: Build skipped due to trigger being Branch Indexing"
//   currentBuild.result = 'success skip' // optional, gives a better hint to the user that it's been skipped, rather than the default which shows it's successful
//   return
// }
def buildNumber = env.BUILD_NUMBER as int
if (buildNumber > 1) milestone(buildNumber - 1)
milestone(buildNumber)
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
//停止之前相同的分支sssss
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
            
          }
        }
        
    }
  }
  } 
}
