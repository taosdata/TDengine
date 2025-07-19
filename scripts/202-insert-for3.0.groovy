case_file="cases_query_replica1.txt"
enterprise_commit_id="12345"
community_commit_id="12345"
def sync_source(tdinternal_branch_name, community_branch_name, internal_root) {
    sh '''
        hostname
        env
        echo ''' + tdinternal_branch_name + '''
    '''
    sh '''
        cd ''' + internal_root + '''
        git reset --hard
        git clean -f
        git fetch || git fetch
        git checkout ''' + tdinternal_branch_name + ''' -f
        git branch
        git pull origin ''' + tdinternal_branch_name + '''
        git log -5
    '''
    sh '''
        cd ''' + internal_root + '''/enterprise/src/plugins/taosx
        [ -e release/taosx ] && rm -rf release/taosx > /dev/null
        git checkout main -f
        git branch
        git pull origin main
        git log -5
        cd ''' + internal_root + '''/enterprise/src/plugins/explorer
        git checkout main -f
        git branch
        git pull origin main
        git log -5
    '''
    sh '''
        cd ''' + internal_root + '''/community
        [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
        [ -e release ] && rm -rf release/*.tar.gz > /dev/null
        git reset --hard
        git clean -f
        git fetch || git fetch
        git checkout ''' + community_branch_name + ''' -f
        git branch
        [ -f src/connector/grafanaplugin/README.md ] && rm -f src/connector/grafanaplugin/README.md > /dev/null || echo "failed to remove grafanaplugin README.md"
        git pull origin ''' + community_branch_name + '''
        git rm --cached tools/taos-tools || :
        git rm --cached tools/taosadapter || :
        git rm --cached tools/taosws-rs || :
        git log -5
        pip3 uninstall taospy -y
        pip3 install taospy
    '''
    return 1
}
def build_package(internal_root, new_version, branch_name) {
    // sh '''
    //     date
    //     cd ${WORK_DIR}/testnglog
    //     rm -rf debug*
    //     if [ -d "${WORK_DIR}/debugNoSan" ]; then
    //        mv ${WORK_DIR}/debugNoSan  ${WORK_DIR}/testnglog
	// 	   echo "Moved debugNoSan to testnglog."
    //     else
    //        echo "Directory ${WORK_DIR}/debugNoSan does not exist. Skipping move."
	// 	fi
    //     if [ -d "${WORK_DIR}/debugSan" ]; then
    //        mv ${WORK_DIR}/debugSan  ${WORK_DIR}/testnglog
	// 	   echo "Moved debugSan to testnglog."
    //     else
    //        echo "Directory ${WORK_DIR}/debugSan does not exist. Skipping move."
	// 	fi
	// '''
	sh '''
        date
        cd ''' + internal_root + '''
        rm -rf /usr/include/taos.h
      
        cp community/include/client/taos.h /usr/include/
        cd enterprise/packaging/
        eval `ssh-agent -s`
        ssh-add
        . $HOME/.cargo/env
        ./new_ver_release.sh -v cluster -n ''' + new_version + ''' -V stable -d no -l full -b ''' + branch_name + ''' -c x64 -s 1 | tee ../ver-3.0.0.100.txt
    '''
    // sh '''
    //     date
    //     rm -rf ${INTERNAL_ROOT}/community/debug
	// 	rm -rf ${INTERNAL_ROOT}/enterprise/contrib/deps-download/CMakeCache.txt
	// 	rm -rf ${INTERNAL_ROOT}/community/contrib/deps-download/CMakeCache.txt
	// 	rm -rf ${INTERNAL_ROOT}/community/contrib/libs3
	// 	rm -rf ${INTERNAL_ROOT}/community/contrib/deps-download/libs3-prefix
	// 	cd ${INTERNAL_ROOT}/community/tests/ci
    //     rm -rf ${INTERNAL_ROOT}/.externals/build/*
    //     time ./container_build_newmachine.sh -w ${WORK_DIR} -e
	// 	cd ${INTERNAL_ROOT}/community/tests/parallel_test
    //     rm -rf ${INTERNAL_ROOT}/.externals/build/*
    //     time ./container_build.sh -w ${WORK_DIR} -e
	// 	rm -rf ${INTERNAL_ROOT}/enterprise/contrib/deps-download/CMakeCache.txt
	// 	rm -rf ${INTERNAL_ROOT}/community/contrib/deps-download/CMakeCache.txt
	// 	rm -rf ${INTERNAL_ROOT}/community/contrib/libs3
	// 	rm -rf ${INTERNAL_ROOT}/community/contrib/deps-download/libs3-prefix
    //     rm -rf ${INTERNAL_ROOT}/.externals/build/*
    // '''
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
    send2feishu = "True"
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
                time ./run.sh -m ''' + host_config_file + ''' -t ''' + case_list_file + ''' -l ${WORK_DIR}/testnglog/''' + date_tag + ''' ''' + extra_flag + ''' -v ${NEW_VERSION} -w ${FILE_WEB_SERVER}'''  + date_tag +  ''' 
            '''
        }
        /*sh '''
            echo "collecting sql ..."
            cd $TESTNG_ROOT/cases/stability/query
            ./collect.sh -l ${WORK_DIR}/testnglog/''' + date_tag + ''' -t $TESTNG_ROOT/cases/stability/query/sql.txt -f "Query" -p "^select"
            time ./run.sh -m ''' + host_config_file + ''' -t ''' + case_list_file + ''' -l ${WORK_DIR}/testnglog/''' + date_tag + ''' ''' + extra_flag + ''' -v ${NEW_VERSION} -w ${FILE_WEB_SERVER}'''  + date_tag + ''' -f  '''  + send2feishu + '''
        '''*/
    } else {
        echo "no case to run"
    }
}
pipeline {
    agent {label " slave202_new "}
    environment{
        WORK_DIR='/var/data/jenkins/workspace'
        INTERNAL_ROOT='/home/branch_test/3.0/TDinternal'
        TAOSTEST_ROOT='/var/data/jenkins/workspace/taos-test-framework'
        TESTNG_ROOT='/var/data/jenkins/workspace/TestNG'
        TDINTERNAL_BRANCH_NAME="${env.TDINTERNAL_TEST_BRANCH}"
        COMMUNITY_BRANCH_NAME="${env.COMMUNITY_TEST_BRANCH}"
        NEW_VERSION='3.0.0.100'
        FILE_WEB_SERVER='http://192.168.0.202:8081/'
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
                                sync_source("${TDINTERNAL_BRANCH_NAME}", "${COMMUNITY_BRANCH_NAME}", "${INTERNAL_ROOT}")
                                build_package("${INTERNAL_ROOT}", "${NEW_VERSION}","${TDINTERNAL_BRANCH_NAME}")
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
                            git reset --hard
                            git fetch
                            git checkout ${TAOS_TEST_BRANCH} -f
                            git pull
                            git log -2
                            echo y|bash reinstall.sh
                        '''
                        sh '''
                            echo "update TestNG ..."
                            cd $TESTNG_ROOT
                            git branch
                            git reset --hard
                            git fetch
                            git checkout ${TAOS_TEST_BRANCH} -f
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
                    run_cases("cases_insert_replica1.txt", "/home/m.json", "1", "", "-s -o 86400")
                }
            }
        }
        stage('replica 3 insert') {
            when {
                expression { env.ENABLE_INSERT_REPLICA3 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_insert_replica3.txt", "/home/m5.json", "3", "", "-s -o 86400")
                }
            }
        }
        stage('replica 1 cluster') {
            when {
                expression { env.ENABLE_CLUSTER_REPLICA1 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_cluster.txt", "/home/m1.json", "1", "", "-s -o 86400")
                }
            }
        }
        stage('replica 3 cluster') {
            when {
                expression { env.ENABLE_CLUSTER_REPLICA3 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_cluster.txt", "/home/m1.json", "3", "", "-s -o 86400")
                }
            }
        }
        stage('long time ci cases') {
            when {
                expression { env.ENABLE_LONGTIME_CI == 'y' }
            }
            steps {
                script {
                    sh '''
                    //     cd ${INTERNAL_ROOT}/community/tests/parallel_test
                    //     export DEFAULT_RETRY_TIME=3
                    //     date
                    //     time ./run.sh -e -m /home/m.json -t longtimeruning_cases.task -b ${TDINTERNAL_BRANCH_NAME}_${BUILD_NUMBER} -l ${WORK_DIR}/testnglog/ -o 2400
                    '''
                }
            }
        }
        stage('all ci cases') {
            when {
                expression { env.ENABLE_ALL_CI == 'y' }
            }
            steps {
                script {
                    sh '''
                    //     cd ${INTERNAL_ROOT}/community/tests/parallel_test
                    //     export DEFAULT_RETRY_TIME=3
                    //     date
                    //     time ./run.sh -e -m /home/m_ci.json -t cases.task -b ${TDINTERNAL_BRANCH_NAME}_${BUILD_NUMBER} -l ${WORK_DIR}/testnglog/ -o 1800
                    '''
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
                    run_cases("cases_query_replica3.txt", "/home/m6.json", "3", "1", "-s -o 30200")
                }
            }
        }
        stage('replica 3 & query policy 3') {
            when {
                expression { env.ENABLE_QUERY_REPLICA3_POLICY3 == 'y' }
            }
            steps {
                script {
                    run_cases("cases_query_replica3.txt", "/home/m6.json", "3", "3", "-s -o 30200")
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
