import hudson.model.Result
import hudson.model.*;
import jenkins.model.CauseOfInterruption
node {
}

def skipbuild = 0
def win_stop = 0
def scope = []
def mod = [0,1,2,3,4]
def sim_mod = [0,1,2,3]

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
    [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
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
    [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
    git pull >/dev/null
    git fetch origin +refs/pull/${CHANGE_ID}/merge
    git checkout -qf FETCH_HEAD
    git clean -dfx
    git submodule update --init --recursive
    cd src/kit/taos-tools/deps/avro
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
    cmake .. -DBUILD_HTTP=false -DBUILD_TOOLS=true > /dev/null
    make > /dev/null
    make install > /dev/null
    cd ${WKC}/tests
    pip3 install taospy
    '''
    return 1
}
def pre_test_noinstall(){
    sh'hostname'
    sh'''
    cd ${WKC}
    [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
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
    [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
    git pull >/dev/null
    git fetch origin +refs/pull/${CHANGE_ID}/merge
    git checkout -qf FETCH_HEAD
    git clean -dfx
    git submodule update --init --recursive
    cd src/kit/taos-tools/deps/avro
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
    cmake .. -DBUILD_HTTP=false -DBUILD_TOOLS=true > /dev/null
    make
    '''
    return 1
}
def pre_test_mac(){
    sh'hostname'
    sh'''
    cd ${WKC}
    [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
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
    [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
    git pull >/dev/null
    git fetch origin +refs/pull/${CHANGE_ID}/merge
    git checkout -qf FETCH_HEAD
    git clean -dfx
    git submodule update --init --recursive
    cd src/kit/taos-tools/deps/avro
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
    cmake .. -DBUILD_TOOLS=false > /dev/null
    go env -w GOPROXY=https://goproxy.cn,direct
    go env -w GO111MODULE=on
    cmake --build .
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
              scope = ['connector','query','insert','other','tools','taosAdapter']
              Collections.shuffle mod
              Collections.shuffle sim_mod
              }
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
        stage('python_1') {
          agent{label " slave1 || slave11 "}
          steps {
            pre_test()
            timeout(time: 100, unit: 'MINUTES'){
              script{
                scope.each {
                  sh """
                    date
                    cd ${WKC}/tests
                    ./test-CI.sh ${it} 5 ${mod[0]}
                    date"""
                  }
                }
            }            
          }
        }
        stage('python_2') {
          agent{label " slave2 || slave12 "}
          steps {
            pre_test()
            timeout(time: 100, unit: 'MINUTES'){
                 script{
                  scope.each {
                    sh """
                      date
                      cd ${WKC}/tests
                      ./test-CI.sh ${it} 5 ${mod[1]} 
                      date"""
                    }
                }
            }
          }
        }
        stage('python_3') {
          agent{label " slave3 || slave13 "}
          steps {
            timeout(time: 105, unit: 'MINUTES'){
              pre_test()
              script{
              scope.each {
                sh """
                  date
                  cd ${WKC}/tests
                  ./test-CI.sh ${it} 5 ${mod[2]}
                  date"""
                }
              }
            }
          }
        }
        stage('python_4') {
          agent{label " slave4 || slave14 "}
          steps {
            timeout(time: 100, unit: 'MINUTES'){
              pre_test()
              script{
              scope.each {
                sh """
                  date
                  cd ${WKC}/tests
                  ./test-CI.sh ${it} 5 ${mod[3]}
                  date"""
                }
              }
          
            }
          }
        }
        stage('python_5') {
          agent{label " slave5 || slave15 "}
          steps {
            timeout(time: 100, unit: 'MINUTES'){
              pre_test()
              script{
              scope.each {
                sh """
                  date
                  cd ${WKC}/tests
                  ./test-CI.sh ${it} 5 ${mod[4]}
                  date"""
                }
              }
          
            }
          }
        }
        stage('sim_1') {
          agent{label " slave6 || slave16 "}
          steps {
            pre_test()
            timeout(time: 100, unit: 'MINUTES'){
                  sh """
                    date
                    cd ${WKC}/tests
                    ./test-CI.sh sim 4 ${sim_mod[0]}
                    date"""
              }
          }            
        }
        stage('sim_2') {
          agent{label " slave7 || slave17 "}
          steps {
            pre_test()
            timeout(time: 100, unit: 'MINUTES'){
              sh """
                date
                cd ${WKC}/tests
                ./test-CI.sh sim 4 ${sim_mod[1]} 
                date"""
            }
          }
        }
        stage('sim_3') {
          agent{label " slave8 || slave18 "}
          steps {
            timeout(time: 105, unit: 'MINUTES'){
              pre_test()
              sh """
                date
                cd ${WKC}/tests
                ./test-CI.sh sim 4 ${sim_mod[2]}
                date"""
            }
          }
        }
        stage('sim_4') {
          agent{label " slave9 || slave19 "}
          steps {
            timeout(time: 100, unit: 'MINUTES'){
              pre_test()
              sh """
                date
                cd ${WKC}/tests
                ./test-CI.sh sim 4 ${sim_mod[3]}
                date"""
              }
            }
          
        }
        stage('other') {
          agent{label " slave10 || slave20 "}
          steps {
            timeout(time: 100, unit: 'MINUTES'){
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
              catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WKC}/tests/pytest
                ./valgrind-test.sh 2>&1 > mem-error-out.log
                ./handle_val_log.sh
                '''
              } 
              sh '''
                cd ${WKC}/tests
                ./test-all.sh full unit
                date
              '''
            }
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
        stage('Mac_build') {
          agent{label " catalina "}
          steps {
              pre_test_mac()
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

