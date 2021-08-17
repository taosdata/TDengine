import hudson.model.Result
import jenkins.model.CauseOfInterruption
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
//停止之前相同的分支
abortPreviousBuilds()
def pre_test(){
    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                sudo rmtaos
                '''
    }

    build.doKill()    //doTerm(),doKill(),doTerm()
  }
}
//  abort previous build
abortPreviousBuilds()
def abort_previous(){
  def buildNumber = env.BUILD_NUMBER as int
  if (buildNumber > 1) milestone(buildNumber - 1)
  milestone(buildNumber)
}
def pre_test(){
    
    
    sh '''
    sudo rmtaos || echo "taosd has not installed"
    '''
    sh '''
    
    cd ${WKC}
    git reset --hard HEAD~10 >/dev/null
    '''
    script {
      if (env.CHANGE_TARGET == 'master') {
        sh '''
        cd ${WKC}
        git checkout master
        '''
        }
      else {
        sh '''
        cd ${WKC}
        git checkout develop
        '''
      } 
    }
    sh'''
    cd ${WKC}
    git pull >/dev/null
    git fetch origin +refs/pull/${CHANGE_ID}/merge
    git checkout -qf FETCH_HEAD
    git clean -dfx
    cd ${WK}
    git reset --hard HEAD~10
    '''
    script {
      if (env.CHANGE_TARGET == 'master') {
        sh '''
        cd ${WK}
        git checkout master
        '''
        }
      else {
        sh '''
        cd ${WK}
        git checkout develop
        '''
      } 
    }
    sh '''
    cd ${WK}
    git pull >/dev/null 

    export TZ=Asia/Harbin
    date
    git clean -dfx
    mkdir debug
    cd debug
    cmake .. > /dev/null
    make > /dev/null
    make install > /dev/null
    cd ${WKC}/tests
    pip3 install ${WKC}/src/connector/python
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
      stage('pre_build'){
          agent{label 'master'}
          when {
              changeRequest()
          }
          steps {
            script{
              abort_previous()
              abortPreviousBuilds()
            }
          sh'''
          rm -rf ${WORKSPACE}.tes
          cp -r ${WORKSPACE} ${WORKSPACE}.tes
          cd ${WORKSPACE}.tes
          
          '''
          script {
            if (env.CHANGE_TARGET == 'master') {
              sh '''
              git checkout master
              git pull origin master
              '''
              }
            else {
              sh '''
              git checkout develop
              git pull origin develop
              '''
            } 
          }
          sh'''
          git fetch origin +refs/pull/${CHANGE_ID}/merge
          git checkout -qf FETCH_HEAD
          '''     
          
          script{
            env.skipstage=sh(script:"cd ${WORKSPACE}.tes && git --no-pager diff --name-only FETCH_HEAD ${env.CHANGE_TARGET}|grep -v -E '.*md|//src//connector|Jenkinsfile|test-all.sh' || echo 0 ",returnStdout:true) 
          }
          println env.skipstage
          sh'''
          rm -rf ${WORKSPACE}.tes
          '''
          }
      }
    
      stage('Parallel test stage') {
        //only build pr
        when {
              changeRequest()
               expression {
                    env.skipstage != 0
              }
          }
      parallel {
        stage('python_1_s1') {
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
