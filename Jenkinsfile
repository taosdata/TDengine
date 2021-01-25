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
    git reset --hard HEAD~10 >/dev/null 
    git pull
    git fetch origin +refs/pull/${CHANGE_ID}/merge
    git checkout -qf FETCH_HEAD
    cd ${WK}
    git reset --hard HEAD~10
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
            timeout(time: 90, unit: 'MINUTES'){
              sh '''
              cd ${WKC}/tests
              find pytest -name '*'sql|xargs rm -rf
              ./test-all.sh p1
              date'''
            }
            
          }
        }
        stage('python_2') {
          agent{label 'p2'}
          steps {
            
            pre_test()
            sh '''
            cd ${WKC}/tests
            find pytest -name '*'sql|xargs rm -rf
            ./test-all.sh p2
            date'''
            sh '''
            cd ${WKC}/tests
            ./test-all.sh b4fq
            '''
          }
        }
        stage('test_b1') {
          agent{label 'b1'}
          steps {     
            timeout(time: 90, unit: 'MINUTES'){       
              pre_test()
              sh '''
              cd ${WKC}/tests
              ./test-all.sh b1fq
              date'''
            }
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
            timeout(time: 90, unit: 'MINUTES'){
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh b2fq
              date
              '''
            }
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
            timeout(time: 90, unit: 'MINUTES'){      
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
  post {      
      
        success {
            emailext (
                subject: "PR-result: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
                body: '''<!DOCTYPE html>
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
                                    <li>构建名称>>分支：${PROJECT_NAME}</li>
                                    <li>构建结果：<span style="color:green"> Successful </span></li>
                                    <li>构建编号：${BUILD_NUMBER}</li>
                                    <li>触发用户：${CAUSE}</li>
                                    <li>提交信息：${CHANGE_TITLE}</li>
                                    <li>构建地址：<a href=${BUILD_URL}>${BUILD_URL}</a></li>
                                    <li>构建日志：<a href=${BUILD_URL}console>${BUILD_URL}console</a></li>
                                    <li>变更集：${JELLY_SCRIPT}</li>
                                </div>
                                </ul>
                            </td>
                        </tr>
                    </table></font>
                </body>
                </html>''',
                to: "${env.CHANGE_AUTHOR_EMAIL}",
                from: "support@taosdata.com"
            )
        }
        failure {
            emailext (
                subject: "PR-result: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
                body: '''<!DOCTYPE html>
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
                                    <li>构建名称>>分支：${PROJECT_NAME}</li>
                                    <li>构建结果：<span style="color:green"> Successful </span></li>
                                    <li>构建编号：${BUILD_NUMBER}</li>
                                    <li>触发用户：${CAUSE}</li>
                                    <li>提交信息：${CHANGE_TITLE}</li>
                                    <li>构建地址：<a href=${BUILD_URL}>${BUILD_URL}</a></li>
                                    <li>构建日志：<a href=${BUILD_URL}console>${BUILD_URL}console</a></li>
                                    <li>变更集：${JELLY_SCRIPT}</li>
                                </div>
                                </ul>
                            </td>
                        </tr>
                    </table></font>
                </body>
                </html>''',
                to: "${env.CHANGE_AUTHOR_EMAIL}",
                from: "support@taosdata.com"
            )
        }
    }
   
}
