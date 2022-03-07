import hudson.model.Result
import hudson.model.*;
import jenkins.model.CauseOfInterruption
node {
}

def skipbuild=0
def win_stop=0

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
//  abort previous build
abortPreviousBuilds()
def abort_previous(){
  def buildNumber = env.BUILD_NUMBER as int
  if (buildNumber > 1) milestone(buildNumber - 1)
  milestone(buildNumber)
}
def pre_test(){
    sh'hostname'
    sh '''
    sudo rmtaos || echo "taosd has not installed"
    '''
    sh '''
    killall -9 taosd ||echo "no taosd running"
    killall -9 gdb || echo "no gdb running"
    killall -9 python3.8 || echo "no python program running"
    cd ${WKC}
    '''
    script {
      if (env.CHANGE_TARGET == 'master') {
        sh '''
        cd ${WKC}
        git checkout master
        '''
        }
      else if(env.CHANGE_TARGET == '2.0'){
        sh '''
        cd ${WKC}
        git checkout 2.0
        '''
      }
      else if(env.CHANGE_TARGET == '3.0'){
        sh '''
        cd ${WKC}
        git checkout 3.0
        '''
      } 
      else{
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
    export TZ=Asia/Harbin
    date
    rm -rf debug
    mkdir debug
    cd debug
    cmake .. > /dev/null
    make -j4> /dev/null

    '''
    return 1
}

pipeline {
  agent none
  options { skipDefaultCheckout() } 
  environment{
      WK = '/var/lib/jenkins/workspace/TDinternal'
      WKC= '/var/lib/jenkins/workspace/TDengine'
  }
  stages {
      stage('pre_build'){
          agent{label 'slave3_0'}
          options { skipDefaultCheckout() } 
          when {
              changeRequest()
          }
          steps {
            script{
              abort_previous()
              abortPreviousBuilds()
            }
            timeout(time: 45, unit: 'MINUTES'){
              pre_test()
              sh'''
              cd ${WKC}/tests
              ./test-all.sh b1fq
              '''
              sh'''
              cd ${WKC}/debug
              ctest
              '''
            }
          }
      }
  }
  post {  
        success {
            emailext (
                subject: "PR-result: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' SUCCESS",
                body: """<!DOCTYPE html>
                <html>
                <head>
                <meta charset="UTF-8">
                </head>
                <body leftmargin="8" marginwidth="0" topmargin="8" marginheight="4" offset="0">
                    <table width="95%" cellpadding="0" cellspacing="0" style="font-size: 16pt; font-family: Tahoma, Arial, Helvetica, sans-serif">
                        <tr>
                            <td><br />
                                <b><font color="#0B610B"><font size="6">构建信息</font></font></b>
                                <hr size="2" width="100%" align="center" /></td>
                        </tr>
                        <tr>
                            <td>
                                <ul>
                                <div style="font-size:18px">
                                    <li>构建名称>>分支：${env.BRANCH_NAME}</li>
                                    <li>构建结果：<span style="color:green"> Successful </span></li>
                                    <li>构建编号：${BUILD_NUMBER}</li>
                                    <li>触发用户：${env.CHANGE_AUTHOR}</li>
                                    <li>提交信息：${env.CHANGE_TITLE}</li>
                                    <li>构建地址：<a href=${BUILD_URL}>${BUILD_URL}</a></li>
                                    <li>构建日志：<a href=${BUILD_URL}console>${BUILD_URL}console</a></li>
                                    
                                </div>
                                </ul>
                            </td>
                        </tr>
                    </table></font>
                </body>
                </html>""",
                to: "${env.CHANGE_AUTHOR_EMAIL}",
                from: "support@taosdata.com"
            )
        }
        failure {
            emailext (
                subject: "PR-result: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' FAIL",
                body: """<!DOCTYPE html>
                <html>
                <head>
                <meta charset="UTF-8">
                </head>
                <body leftmargin="8" marginwidth="0" topmargin="8" marginheight="4" offset="0">
                    <table width="95%" cellpadding="0" cellspacing="0" style="font-size: 16pt; font-family: Tahoma, Arial, Helvetica, sans-serif">
                        <tr>
                            <td><br />
                                <b><font color="#0B610B"><font size="6">构建信息</font></font></b>
                                <hr size="2" width="100%" align="center" /></td>
                        </tr>
                        <tr>
                            <td>
                                <ul>
                                <div style="font-size:18px">
                                    <li>构建名称>>分支：${env.BRANCH_NAME}</li>
                                    <li>构建结果：<span style="color:red"> Failure </span></li>
                                    <li>构建编号：${BUILD_NUMBER}</li>
                                    <li>触发用户：${env.CHANGE_AUTHOR}</li>
                                    <li>提交信息：${env.CHANGE_TITLE}</li>
                                    <li>构建地址：<a href=${BUILD_URL}>${BUILD_URL}</a></li>
                                    <li>构建日志：<a href=${BUILD_URL}console>${BUILD_URL}console</a></li>
                                    
                                </div>
                                </ul>
                            </td>
                        </tr>
                    </table></font>
                </body>
                </html>""",
                to: "${env.CHANGE_AUTHOR_EMAIL}",
                from: "support@taosdata.com"
            )
        }
    } 
}
