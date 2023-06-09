case_file="cases_query_repilca1.txt"
enterprise_commit_id="12345"
community_commit_id="12345"
def sync_source(branch_name, internal_root) {
    sh '''
        hostname
        env
        echo ''' + branch_name + '''
    '''
    sh '''
        cd ''' + internal_root + '''
        git reset --hard
        git fetch || git fetch
        git checkout ''' + branch_name + ''' -f
        git branch
        git pull || git pull
        git clean -dfx
        git log -5
    '''
    sh '''
        cd ''' + internal_root + '''/community
        [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
        git reset --hard
        git fetch || git fetch
        git checkout ''' + branch_name + ''' -f
        git clean -dfx
        git branch
        [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
        git pull || git pull
        git rm --cached tools/taos-tools || :
        git rm --cached tools/taosadapter || :
        git rm --cached tools/taosws-rs || :
        git log -5
        rm -rf src/connector/python
        mkdir -p src/connector/python
        git clone https://github.com/taosdata/taos-connector-python.git src/connector/python || git clone https://github.com/taosdata/taos-connector-python.git src/connector/python
    '''
    return 1
}
def build_package(internal_root, new_version) {
    sh '''
        date
        cd ''' + internal_root + '''
        rm -rf /usr/include/taos.h
        cp community/include/client/taos.h /usr/include/
        cd enterprise/packaging/
        eval `ssh-agent -s`
        ssh-add
        ./new_ver_release.sh -v cluster -n ''' + new_version + ''' -V stable -d no -l full -b 3.0 -c x64
    '''
}
def check_cases_to_run() {
    def exitValue = 0
    if (env.FAILED_ONLY == "y") {
        exitValue = sh(script: "cd ${WORK_DIR}/testnglog;ls|grep ${JOB_NAME}|sort|tail -n1|xargs ls|grep failed.txt", returnStatus: true)
    }
    return exitValue
}
def change_case_file(sub_folder) {
    if (env.FAILED_ONLY == "y") {
        def last_log_dir = sh (
            script: 'cd ${WORK_DIR}/testnglog;ls|grep ${JOB_NAME}|sort|tail -n1',
            returnStdout: true
        ).trim()
        echo "last log dir: [" + last_log_dir + "]"
        case_file="${WORK_DIR}/testnglog/" + sub_folder + "/last_failed_cases.txt"
        sh '''
            mkdir -p ${WORK_DIR}/testnglog/''' + sub_folder + '''
            cat ${WORK_DIR}/testnglog/''' + last_log_dir + '''/failed.txt | grep -w taostest | sed "s/^.* ret:[0-9]* //" >''' + case_file + '''
            echo "***** cases to run *****"
            cat ''' + case_file + '''
        '''
    }
}
def run_cases(case_list_file, host_config_file, replicas, query_policy, extra_flag) {
    sh '''
        hostname
        date
        rm -f /tmp/${JOB_NAME}.env
        touch /tmp/${JOB_NAME}.env
    '''
    // internal_root='/var/data/jenkins/workspace/TDinternal'
    send2feishu = "true"
    print(check_cases_to_run())
    if (check_cases_to_run() == 0) {
        def date_tag = sh (
            script: 'date +%Y%m%d-%H%M%S',
            returnStdout: true
        ).trim()
        date_tag = "${JOB_NAME}_${BUILD_NUMBER}_" + date_tag
        change_case_file(date_tag)
        if (replicas != "") {
            sh '''
                echo "export DATABASE_REPLICAS=''' + replicas + '''" >>/tmp/${JOB_NAME}.env
            '''
        }
        if (query_policy != "") {
            sh '''
                echo "export DATABASE_QUERY_POLICY=''' + query_policy + '''" >>/tmp/${JOB_NAME}.env
            '''
        }
        // add parameters of sending to feishu robot 
        sh '''
            cd ${INTERNAL_ROOT}
            git log -5 |grep commit| head -1|awk '{print $2}'  > tmp1.csv
            cd ${INTERNAL_ROOT}/community
            git log -5 |grep commit| head -1|awk '{print $2}'  > tmp2.csv
        '''
        def enterprise_commit_id = sh (
            script: 'cd ${INTERNAL_ROOT};cat  tmp1.csv',
            returnStdout: true
        ).trim()
        // def community_commit_id = sh (
        //     script: 'cd ${INTERNAL_ROOT}/community;git log -5 |grep commit| head -1|awk "{print $2}"',
        //     returnStdout: true
        // ).trim()
        def community_commit_id = sh (
            script: 'cd ${INTERNAL_ROOT}/community;cat tmp2.csv',
            returnStdout: true
        ).trim()
        echo "enterprise_commit_id: [" + enterprise_commit_id + "]"
        echo "community_commit_id: [" + community_commit_id + "]"
        sh '''
            echo "export JOB_NAME=${JOB_NAME} " >>/tmp/${JOB_NAME}.env
            echo "export BUILD_NUMBER=${BUILD_NUMBER} " >>/tmp/${JOB_NAME}.env  
            echo "export community_commit_id=''' + community_commit_id + '''" >>/tmp/${JOB_NAME}.env
            echo "export enterprise_commit_id=''' + enterprise_commit_id + '''" >>/tmp/${JOB_NAME}.env                        
        '''

        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
            sh '''
                echo "run taostest ..."
                mkdir -p ${WORK_DIR}/testnglog/''' + date_tag + '''
                cp -rf ${INTERNAL_ROOT}/community/release ${WORK_DIR}/testnglog/''' + date_tag + '''
                cd $TESTNG_ROOT/scripts
                source /tmp/${JOB_NAME}.env
                time ./run.sh -m ''' + host_config_file + ''' -t ''' + case_list_file + ''' -l ${WORK_DIR}/testnglog/''' + date_tag + ''' -d debug ''' + extra_flag + ''' -v ${NEW_VERSION} -w ${FILE_WEB_SERVER}'''  + date_tag + ''' -f  '''  + send2feishu + ''' 
            '''
        }
        /*sh '''
            echo "collecting sql ..."
            cd $TESTNG_ROOT/cases/stability/query
            ./collect.sh -l ${WORK_DIR}/testnglog/''' + date_tag + ''' -t $TESTNG_ROOT/cases/stability/query/sql.txt -f "Query" -p "^select"
        '''*/
    } else {
        echo "no case to run"
    }
}
pipeline {
    agent {label " slave218 "}
    environment{
        WORK_DIR='/var/data/jenkins/workspace'
        INTERNAL_ROOT='/var/data/jenkins/workspace/TDinternal'
        TAOSTEST_ROOT='/var/data/jenkins/workspace/taos-test-framework'
        TESTNG_ROOT='/var/data/jenkins/workspace/TestNG'
        BRANCH_NAME="${env.TEST_BRANCH}"
        NEW_VERSION='3.0.0.100'
        FILE_WEB_SERVER='http://192.168.0.218:8081/'
    }
    stages {
        stage ('build source') {
            when {
                expression { env.SKIP_REBUILD == 'n' }
            }
            parallel {
                stage('linux build') {
                    steps {
                        script {
                            sh '''
                                hostname
                                date
                                service docker restart
                                
                                
                            '''
                            if (check_cases_to_run() == 0) {
                                sync_source("${BRANCH_NAME}", "${INTERNAL_ROOT}")
                                build_package("${INTERNAL_ROOT}", "${NEW_VERSION}")
                            } else {
                                echo "no case to run"
                            }
                        }
                    }
                }
            }
        }
        stage ('update') {
            steps {
                script {
                    sh '''
                        hostname
                        date
                    '''
                    if (check_cases_to_run() == 0) {
                        sh '''
                            echo "install taostest ..."
                            cd ${TAOSTEST_ROOT}
                            git branch
                            git pull
                            git log -2
                            echo y|bash reinstall.sh
                        '''
                        sh '''
                            echo "update TestNG ..."
                            cd $TESTNG_ROOT
                            git branch
                            git reset --hard
                            git pull
                            git log -2
                        '''
                    }
                }
            }
        }
        stage('replica 1 insert') {
            when {
                expression { env.ENABLE_INSERT_REPLICA1 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_insert_replica1.txt", "/home/m.json", "1", "", "-s -o 3600")
                }
            }
        }
        stage('replica 3 insert') {
            when {
                expression { env.ENABLE_INSERT_REPLICA3 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_insert_replica3.txt", "/home/m5.json", "3", "", "-s -o 3600")
                }
            }
        }
        stage('taosx') {
            when {
                expression { env.ENABLE_TAOSX == 'y' }
            }
            steps {
                script {
                    run_cases("cases_taosx_fulltest.txt", "/home/m1.json", "3", "1", "-s -o 3600")
                }
            }
        }
        stage('replica 1 query') {
            when {
                expression { env.ENABLE_QUERY_REPLICA1 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_query_replica1.txt", "/home/m.json", "1", "1", "-s -o 7200")
                }
            }
        }
        stage('replica 3 & query policy 1') {
            when {
                expression { env.ENABLE_QUERY_REPLICA3_POLICY1 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_query_replica3.txt", "/home/m5.json", "3", "1", "-s -o 7200")
                }
            }
        }
        stage('replica 3 & query policy 3') {
            when {
                expression { env.ENABLE_QUERY_REPLICA3_POLICY3 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_query_replica3.txt", "/home/m5.json", "3", "3", "-s -o 7200")
                }
            }
        }
        /*
        stage('insert & query') {
            when {
                expression { env.ENABLE_INSERT_QUERY == 'y' }
            }
            parallel {
                stage('insert') {
                    steps {
                        script {
                            run_cases("stability/cases.g12.txt", "/home/m1.json", "", "", "")
                        }
                    }
                }
                stage('query') {
                    steps {
                        script {
                            sh '''
                                export TEST_ROOT=${TESTNG_ROOT}
                                sleep 180
                                valgrind --log-file=${TESTNG_ROOT}/val.${BUILD_NUMBER}.log taostest --use=common_cluster_30.yaml --case=Query/queryscript/scene_query/meters/meters_query.py --keep --disable_collection --containers --docker-network=taosnet_0 --log-level=info
                            '''
                        }
                    }
                }
            }
        }
        */
        
    }
    post {
        success {
            emailext (
                subject: "${env.JOB_NAME}-RESULT: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' SUCCESS",
                body: """<!DOCTYPE html>
                    <html>
                        <head>
                            <meta charset="UTF-8">
                        </head>
                        <body leftmargin="8" marginwidth="0" topmargin="8" marginheight="4" offset="0">
                            <table width="95%" cellpadding="0" cellspacing="0" style="font-size: 16pt; font-family: Tahoma, Arial, Helvetica, sans-serif">
                                <tr>
                                    <td>
                                        <br/>
                                        <b><font color="#0B610B"><font size="6">全量测试3.0构建信息</font></font></b>
                                        <hr size="2" width="100%" align="center" />
                                     </td>
                                </tr>
                                <tr>
                                    <td>
                                        <ul>
                                            <div style="font-size:18px">
                                                <li>构建结果：<span style="color:green"> Successful </span></li>
                                                <li>构建编号：${BUILD_NUMBER}</li>
                                                <li>构建地址：<a href=${BUILD_URL}>${BUILD_URL}</a></li>
                                                <li>构建日志：<a href=${BUILD_URL}console>${BUILD_URL}console</a></li>
                                                <li>构建详情：<a href=http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/${JOB_NAME}/detail/${JOB_NAME}/${BUILD_NUMBER}/pipeline>http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/${JOB_NAME}/detail/${JOB_NAME}/${BUILD_NUMBER}/pipeline</a></li>
                                            </div>
                                        </ul>
                                    </td>
                                </tr>
                            </table>
                        </body>
                    </html>""",
                to: "fztang@taosdata.com, huili@taosdata.com, wxzhang@taosdata.com, cyjia@taosdata.com, cpwu@taosdata.com, hrchen@taosdata.com, jbjia@taosdata.com, xyguo@taosdata.com, zwen@taosdata.com, zqwang@taosdata.com",
                from: "support@taosdata.com"
            )
        }
        failure {
            emailext (
                subject: "${env.JOB_NAME}-RESULT: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' FAILURE",
                body: """<!DOCTYPE html>
                    <html>
                        <head>
                            <meta charset="UTF-8">
                        </head>
                        <body leftmargin="8" marginwidth="0" topmargin="8" marginheight="4" offset="0">
                            <table width="95%" cellpadding="0" cellspacing="0" style="font-size: 16pt; font-family: Tahoma, Arial, Helvetica, sans-serif">
                                <tr>
                                    <td>
                                        <br/>
                                        <b><font color="#0B610B"><font size="6">全量测试3.0构建信息</font></font></b>
                                        <hr size="2" width="100%" align="center" />
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        <ul>
                                            <div style="font-size:18px">
                                                <li>构建结果：<span style="color:red"> Failure </span></li>
                                                <li>构建编号：${BUILD_NUMBER}</li>
                                                <li>构建地址：<a href=${BUILD_URL}>${BUILD_URL}</a></li>
                                                <li>构建日志：<a href=${BUILD_URL}console>${BUILD_URL}console</a></li>
                                                <li>构建详情：<a href=http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/${JOB_NAME}/detail/${JOB_NAME}/${BUILD_NUMBER}/pipeline>http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/${JOB_NAME}/detail/${JOB_NAME}/${BUILD_NUMBER}/pipeline</a></li>
                                            </div>
                                        </ul>
                                    </td>
                                </tr>
                            </table>
                        </body>
                    </html>""",
                to: "fztang@taosdata.com, huili@taosdata.com, wxzhang@taosdata.com, cyjia@taosdata.com, cpwu@taosdata.com, hrchen@taosdata.com, jbjia@taosdata.com, xyguo@taosdata.com, zwen@taosdata.com, zqwang@taosdata.com",
                from: "support@taosdata.com"
            )
        }
    }
}