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
            make install > /dev/null
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
            '''
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WORKSPACE}/tests/gotest
                bash batchtest.sh
                '''
            }
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WORKSPACE}/tests/examples/python/PYTHONConnectorChecker
                python3 PythonChecker.py
                '''
            }
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${WORKSPACE}/tests/examples/JDBC/JDBCDemo/
                mvn clean package assembly:single >/dev/null 
                java -jar target/jdbcChecker-SNAPSHOT-jar-with-dependencies.jar -host 127.0.0.1
                '''
            }
            catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                sh '''
                cd ${JENKINS_HOME}/workspace/C#NET/src/CheckC#
                dotnet run
                '''
            }
          
         }
       }

      }
    }

  }
  post {             
        success {
            emailext (
                subject: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
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
                                    <li>变更概要：${CHANGES}</li>
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
                to: "yqliu@taosdata.com,pxiao@taosdata.com",
                from: "support@taosdata.com"
            )
        }
        failure {
            emailext (
                subject: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
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
                                    <li>变更概要：${CHANGES}</li>
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
                to: "yqliu@taosdata.com,pxiao@taosdata.com",
                from: "support@taosdata.com"
            )
        }
    }
}