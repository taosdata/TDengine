import hudson.model.Result
import jenkins.model.CauseOfInterruption
properties([pipelineTriggers([githubPush()])])
node {
    git url: 'https://github.com/taosdata/TDengine.git'
}

def skipbuild=0

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
    git reset --hard HEAD~10 >/dev/null
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
      else if(env.CHANGE_TARGET == '2.0'){
        sh '''
        cd ${WK}
        git checkout 2.0
        '''
      } 
      else{
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
    pip3 install ${WKC}/src/connector/python/
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
          git fetch
          '''
          script {
            if (env.CHANGE_TARGET == 'master') {
              sh '''
              git checkout master
              '''
              }
            else if(env.CHANGE_TARGET == '2.0'){
              sh '''
              git checkout 2.0
              '''
            } 
            else{
              sh '''
              git checkout develop
              '''
            } 
          }
          sh'''
          git fetch origin +refs/pull/${CHANGE_ID}/merge
          git checkout -qf FETCH_HEAD
          '''     

          script{  
            skipbuild='2'     
            skipbuild=sh(script: "git log -2 --pretty=%B | fgrep -ie '[skip ci]' -e '[ci skip]' && echo 1 || echo 2", returnStdout:true)
            println skipbuild
          }
          sh'''
          rm -rf ${WORKSPACE}.tes
          '''
          }
      }
      stage('Parallel test stage') {
        //only build pr
        when {
          allOf{
              changeRequest()
               expression{
                return skipbuild.trim() == '2'
              }
            }
          }
      parallel {
        stage('python_1_s1') {
          agent{label " slave1 || slave11 "}
          steps {
            
            pre_test()
            timeout(time: 55, unit: 'MINUTES'){
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh p1
              date'''
            }
            
          }
        }
        stage('python_2_s5') {
          agent{label " slave5 || slave15 "}
          steps {
            
            pre_test()
            timeout(time: 55, unit: 'MINUTES'){
                sh '''
                date
                cd ${WKC}/tests
                ./test-all.sh p2
                date'''
            }
          }
        }
        stage('python_3_s6') {
          agent{label " slave6 || slave16 "}
          steps {     
            timeout(time: 55, unit: 'MINUTES'){       
              pre_test()
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh p3
              date'''
            }
          }
        }
        stage('test_b1_s2') {
          agent{label " slave2 || slave12 "}
          steps {     
            timeout(time: 55, unit: 'MINUTES'){       
              pre_test()
              sh '''
              cd ${WKC}/tests
              ./test-all.sh b1fq
              date'''
            }
          }
        }
        stage('test_crash_gen_s3') {
          agent{label " slave3 || slave13 "}
          
          steps {
            pre_test()
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 60, unit: 'MINUTES'){
                  sh '''
                  cd ${WKC}/tests/pytest
                  ./crash_gen.sh -a -p -t 4 -s 2000
                  '''
                }
            }
            timeout(time: 60, unit: 'MINUTES'){
              sh '''
              cd ${WKC}/tests/pytest
              rm -rf /var/lib/taos/*
              rm -rf /var/log/taos/*
              ./handle_crash_gen_val_log.sh
              '''
              sh '''
              cd ${WKC}/tests/pytest
              rm -rf /var/lib/taos/*
              rm -rf /var/log/taos/*
              ./handle_taosd_val_log.sh
              '''
            }
            timeout(time: 55, unit: 'MINUTES'){
                sh '''
                date
                cd ${WKC}/tests
                ./test-all.sh b2fq
                date
                '''
            }                     
          }
        }
        stage('test_valgrind_s4') {
          agent{label " slave4 || slave14 "}

          steps {
            pre_test()
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WKC}/tests/pytest
                ./valgrind-test.sh 2>&1 > mem-error-out.log
                ./handle_val_log.sh
                '''
            }     
            timeout(time: 55, unit: 'MINUTES'){      
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh b3fq
              date'''
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh full example
              date'''
            }
          }
        }
        stage('test_b4_s7') {
          agent{label " slave7 || slave17 "}
          steps {     
            timeout(time: 55, unit: 'MINUTES'){       
              pre_test()
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh b4fq
              cd ${WKC}/tests
              ./test-all.sh p4
              cd ${WKC}/tests
              ./test-all.sh full jdbc
              cd ${WKC}/tests
              ./test-all.sh full unit
              date'''
            }
          }
        }
        stage('test_b5_s8') {
          agent{label " slave8 || slave18 "}
          steps {     
            timeout(time: 55, unit: 'MINUTES'){       
              pre_test()
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh b5fq
              date'''
            }
          }
        }
        stage('test_b6_s9') {
          agent{label " slave9 || slave19 "}
          steps {     
            timeout(time: 55, unit: 'MINUTES'){       
              pre_test()
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh b6fq
              date'''
            }
          }
        }
        stage('test_b7_s10') {
          agent{label " slave10 || slave20 "}
          steps {     
            timeout(time: 55, unit: 'MINUTES'){       
              pre_test()
              sh '''
              date
              cd ${WKC}/tests
              ./test-all.sh b7fq
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