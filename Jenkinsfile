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
    kill -9 $(pidof taosd) ||echo "no taosd running"
    kill -9 $(pidof taosadapter) ||echo "no taosadapter running"
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
    git remote prune origin 
    git pull >/dev/null
    git fetch origin +refs/pull/${CHANGE_ID}/merge
    git checkout -qf FETCH_HEAD
    git clean -dfx
    git submodule update --init --recursive
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
def pre_test_noinstall(){
    sh'hostname'
    sh'''
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
    git remote prune origin 
    git pull >/dev/null
    git fetch origin +refs/pull/${CHANGE_ID}/merge
    git checkout -qf FETCH_HEAD
    git clean -dfx
    git submodule update --init --recursive
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
    make
    '''
    return 1
}
def pre_test_ningsi(){
    sh'hostname'
    sh'''
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
    git remote prune origin 
    git pull >/dev/null
    git fetch origin +refs/pull/${CHANGE_ID}/merge
    git checkout -qf FETCH_HEAD
    git clean -dfx
    git submodule update --init --recursive
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
    cmake .. -DOSTYPE=Ningsi60 > /dev/null
    make
    '''
    return 1
}
def pre_test_win(){
    bat '''
    taskkill /f /t /im python.exe
    cd C:\\
    rd /s /Q C:\\TDengine
    cd C:\\workspace\\TDinternal
    rd /s /Q C:\\workspace\\TDinternal\\debug
    cd C:\\workspace\\TDinternal\\community
    git reset --hard HEAD~10 
    '''
    script {
      if (env.CHANGE_TARGET == 'master') {
        bat '''
        cd C:\\workspace\\TDinternal\\community
        git checkout master
        '''
        }
      else if(env.CHANGE_TARGET == '2.0'){
        bat '''
        cd C:\\workspace\\TDinternal\\community
        git checkout 2.0
        '''
      } 
      else{
        bat '''
        cd C:\\workspace\\TDinternal\\community
        git checkout develop
        '''
      }
    }
    bat'''
    cd C:\\workspace\\TDinternal\\community
    git remote prune origin 
    git pull 
    git fetch origin +refs/pull/%CHANGE_ID%/merge
    git checkout -qf FETCH_HEAD
    git clean -dfx
    git submodule update --init --recursive
    cd C:\\workspace\\TDinternal
    git reset --hard HEAD~10
    '''
    script {
      if (env.CHANGE_TARGET == 'master') {
        bat '''
        cd C:\\workspace\\TDinternal
        git checkout master
        '''
        }
      else if(env.CHANGE_TARGET == '2.0'){
        bat '''
        cd C:\\workspace\\TDinternal
        git checkout 2.0
        '''
      } 
      else{
        bat '''
        cd C:\\workspace\\TDinternal
        git checkout develop
        '''
      } 
    }
    bat '''
    cd C:\\workspace\\TDinternal
    git pull 

    date
    git clean -dfx
    mkdir debug
    cd debug
    call "C:\\Program Files (x86)\\Microsoft Visual Studio\\2017\\Community\\VC\\Auxiliary\\Build\\vcvarsall.bat" amd64
    cmake ../ -G "NMake Makefiles" 
    set CL=/MP nmake nmake || exit 8
    nmake install || exit 8
    xcopy /e/y/i/f C:\\workspace\\TDinternal\\debug\\build\\lib\\taos.dll C:\\Windows\\System32 || exit 8
    cd C:\\workspace\\TDinternal\\community\\src\\connector\\python
    python -m pip install .
    
    '''
    return 1
}
pipeline {
  agent none
  options { skipDefaultCheckout() } 
  environment{
      WK = '/var/lib/jenkins/workspace/TDinternal'
      WKC= '/var/lib/jenkins/workspace/TDinternal/community'
  }
  stages {
      stage('pre_build'){
          agent{label 'master'}
          options { skipDefaultCheckout() } 
          when {
              changeRequest()
          }
          steps {
            script{
              abort_previous()
              abortPreviousBuilds()
            }
          //   sh'''
          // rm -rf ${WORKSPACE}.tes
          // cp -r ${WORKSPACE} ${WORKSPACE}.tes
          // cd ${WORKSPACE}.tes
          // git fetch
          // '''
          // script {
          //   if (env.CHANGE_TARGET == 'master') {
          //     sh '''
          //     git checkout master
          //     '''
          //     }
          //   else if(env.CHANGE_TARGET == '2.0'){
          //     sh '''
          //     git checkout 2.0
          //     '''
          //   } 
          //   else{
          //     sh '''
          //     git checkout develop
          //     '''
          //   } 
          // }
          // sh'''
          // git fetch origin +refs/pull/${CHANGE_ID}/merge
          // git checkout -qf FETCH_HEAD
          // '''     

          // script{  
          //   skipbuild='2'     
          //   skipbuild=sh(script: "git log -2 --pretty=%B | fgrep -ie '[skip ci]' -e '[ci skip]' && echo 1 || echo 2", returnStdout:true)
          //   println skipbuild
          // }
          // sh'''
          // rm -rf ${WORKSPACE}.tes
          // '''
          // }       
          }
      }
      stage('Parallel test stage') {
        //only build pr
        options { skipDefaultCheckout() } 
        when {
          allOf{
              changeRequest()
              not{ expression { env.CHANGE_BRANCH =~ /docs\// }}
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
            timeout(time: 65, unit: 'MINUTES'){       
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
                rm -rf /var/lib/taos/*
                rm -rf /var/log/taos/*
                nohup taosd >/dev/null &
                sleep 10
              '''
              sh '''
              cd ${WKC}/tests/examples/nodejs
              npm install td2.0-connector > /dev/null 2>&1
              node nodejsChecker.js host=localhost
              '''
              catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                  cd ${WKC}/tests/examples/C#/taosdemo
                  mcs -out:taosdemo *.cs > /dev/null 2>&1
                  echo '' |./taosdemo -c /etc/taos
                '''
              } 
              sh '''
                cd ${WKC}/tests/gotest
                bash batchtest.sh
              '''
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
            timeout(time: 60, unit: 'MINUTES'){
              sh '''
              cd ${WKC}/tests/pytest
              ./crash_gen.sh -a -p -t 4 -s 2000
              '''
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
            timeout(time: 105, unit: 'MINUTES'){       
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
            timeout(time: 105, unit: 'MINUTES'){       
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
        stage('arm64centos7') {
          agent{label " arm64centos7 "}
          steps {     
              pre_test_noinstall()    
            }
        }
        stage('arm64centos8') {
          agent{label " arm64centos8 "}
          steps {     
              pre_test_noinstall()    
            }
        }
        stage('arm32bionic') {
          agent{label " arm32bionic "}
          steps {     
              pre_test_noinstall()    
            }
        }
        stage('arm64bionic') {
          agent{label " arm64bionic "}
          steps {     
              pre_test_noinstall()    
            }
        }
        stage('arm64focal') {
          agent{label " arm64focal "}
          steps {     
              pre_test_noinstall()    
            }
        }
        stage('centos7') {
          agent{label " centos7 "}
          steps {     
              pre_test_noinstall()    
            }
        }
        stage('ubuntu:trusty') {
          agent{label " trusty "}
          steps {     
              pre_test_noinstall()    
            }
        }
        stage('ubuntu:xenial') {
          agent{label " xenial "}
          steps {     
              pre_test_noinstall()    
            }
        }
        stage('ubuntu:bionic') {
          agent{label " bionic "}
          steps {     
              pre_test_noinstall()    
            }
        }

        stage('ningsi') {
          agent{label "ningsi"}
          steps {     
              pre_test_ningsi()    
            }
        }
        
        stage('build'){
          agent{label " wintest "}
          steps {
            pre_test()
            script{             
                while(win_stop == 0){
                  sleep(1)
                  }
              }
            }
        }
        stage('test'){
          agent{label "win"}
          steps{
            
            catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                pre_test_win()
                timeout(time: 20, unit: 'MINUTES'){
                bat'''
                cd C:\\workspace\\TDinternal\\community\\tests\\pytest
                .\\test-all.bat wintest
                '''
                }
            }     
            script{
              win_stop=1
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
