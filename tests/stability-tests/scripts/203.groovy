test_loop=1
case_file=""
env_docker_network_map_file = "cases.env.map.txt"
m_config_file="/home/mc.json"
date_tag=""
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
        git clone git@github.com:taosdata/taos-connector-python src/connector/python || git clone git@github.com:taosdata/taos-connector-python src/connector/python
    '''
    return 1
}
def build_package(internal_root, new_version) {
    sh '''
        date
        cd ''' + internal_root + '''
        rm -rf /usr/include/taos.h
        cp community/include/client/taos.h /usr/include/
        cd community/packaging
        eval `ssh-agent -s`
        ssh-add
        ./release.sh -v cluster -n ''' + new_version + ''' -s static
        #docker run -v /var/data/jenkins/workspace/TDinternal:/home/TDinternal --rm taos_test:v1.1 sh -c "cp /home/TDinternal/community/include/client/taos.h /usr/include/; cd /home/TDinternal/community/packaging; ./release.sh -n 3.0.0.100 -s static -v cluster"
    '''
}
def run_cases(case_list_file, host_config_file, max_time_seconds, extra_flag, replicas, query_policy) {
    sh '''
        hostname
        date
        rm -f /tmp/${JOB_NAME}.env
        touch /tmp/${JOB_NAME}.env
    '''
    date_tag = sh (
        script: 'date +%Y%m%d-%H%M%S',
        returnStdout: true
    ).trim()
    date_tag = "${JOB_NAME}_${BUILD_NUMBER}_" + case_list_file + "_" + date_tag
    if (env.ENV_FILE != "") {
        extra_flag = " -E ${env.ENV_FILE} " + extra_flag
    }
    if (env.MNODE_COUNT != "") {
        extra_flag = " -c ${env.MNODE_COUNT} " + extra_flag
    }
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
    send2feishu = "true"
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
            time ./run.sh -m ''' + host_config_file + ''' -t ''' + case_list_file + ''' -l ${WORK_DIR}/testnglog/''' + date_tag + ''' -d debug ''' + extra_flag + ''' -v ${NEW_VERSION} -o ''' + max_time_seconds + ''' -w ${HTTP_SERVICE}'''  + date_tag + ''' -M ''' + env_docker_network_map_file + ''' -f  '''  + send2feishu + ''' 
        '''
    }
}
def get_test_loop() {
    if (env.TEST_LOOP != "") {
        try {
            test_loop = env.TEST_LOOP as int
            echo "test loop: " + test_loop
        } catch(all) {
            echo "TEST_LOOP [$TEST_LOOP] not an integer"
            sh '''
                exit 1
            '''
        }
    }
}
def create_case_list(case_list_file, keyword) {
    def tmp_case_file = case_list_file + ".tmp.txt"
    def real_case_file = case_list_file + ".real.txt"
    sh '''
        cd $TESTNG_ROOT/scripts
        rm -f ''' + tmp_case_file + '''
        rm -f ''' + real_case_file + '''
        grep -E "''' + keyword + '''" ''' + case_list_file + ''' | sed "s/^taostest/taostest --disable_collection/" | tail -n 20000 > ''' + tmp_case_file + '''
    '''
    for(int i=0; i<test_loop; i++) {
        sh '''
            cd $TESTNG_ROOT/scripts
            cat ''' + tmp_case_file + ''' >>''' + real_case_file + '''
        '''
    }
    return real_case_file
}
def init_m_config() {
    sh '''
        sed -i "s/\\"thread\\":.*/\\"thread\\": $TEST_CONCURRENCY/" ''' + m_config_file + '''
    '''
}
def create_env_case_list(case_list_file) {
    def env_case_file = "cases.env.setup.txt"
    sh '''
        cd $TESTNG_ROOT/scripts
        rm -f ''' + env_case_file + '''
    '''
    if (env.ENV_FILE == "") {
        sh '''
            cd $TESTNG_ROOT/scripts
            cat ''' + case_list_file + ''' |grep -o "\\\\-\\\\-setup=.*"|awk '{print $1}'|sed "s/--setup=//"|uniq >> ''' + env_case_file + '''
            cat ''' + case_list_file + ''' |grep -o "\\\\-\\\\-use=.*"|awk '{print $1}'|sed "s/--use=//"|uniq >> ''' + env_case_file + '''
            cat ''' + env_case_file + ''' |uniq > ''' + env_docker_network_map_file + '''
        '''
    } else {
        sh '''
            cd $TESTNG_ROOT/scripts
            echo "''' + env.ENV_FILE + '''" > ''' + env_docker_network_map_file + '''
        '''
    }
    sh '''
        cd $TESTNG_ROOT/scripts
        cp ''' + env_docker_network_map_file + ''' ''' + env_case_file + '''
        rm -f ''' + env_docker_network_map_file + '''
        while read line; do
            var=`echo "${line}" | sed "s/\\//./g"`
            echo "${line} ${JOB_NAME}.${var}net" >>''' + env_docker_network_map_file + '''
        done <''' + env_case_file + '''
    '''
    sh '''
        cd $TESTNG_ROOT/scripts
        sed -i "s/^/taostest --setup=/" ''' + env_case_file + '''
        sed -i "s/$/ --case=helloworld\\\\/hellocase.py --keep --containers/" ''' + env_case_file + '''
    '''
    return env_case_file
}
pipeline {
    agent {label " slave203 "}
    environment{
        WORK_DIR="/var/data/jenkins/workspace"
        INTERNAL_ROOT="${WORK_DIR}/TDinternal"
        TAOSTEST_ROOT="${WORK_DIR}/taos-test-framework"
        TESTNG_ROOT="${WORK_DIR}/TestNG"
        HTTP_SERVICE='http://192.168.0.203:8081/'
        BRANCH_NAME="${env.TEST_BRANCH}"
        //NEW_VERSION='3.9.9.9'
        NEW_VERSION='3.0.0.100'
    }
    stages {
        stage ('build source') {
            parallel {
                stage('linux build') {
                    when {
                        expression { env.SKIP_REBUILD == 'n' }
                    }
                    steps {
                        script {
                            sh '''
                                hostname
                                date
                            '''
                            sync_source("${BRANCH_NAME}", "${INTERNAL_ROOT}")
                            build_package("${INTERNAL_ROOT}", "${NEW_VERSION}")
                            sh '''
                                echo "update taostest ..."
                                cd $TESTNG_ROOT/scripts
                                git branch
                                git pull
                            '''
                            sh '''
                                echo "install taostest ..."
                                cd ${TAOSTEST_ROOT}
                                git branch
                                git pull
                                echo y|bash reinstall.sh
                            '''
                        }
                    }
                }
            }
        }
        stage('setup') {
            steps {
                script {
                    init_m_config()
                    get_test_loop()
                    // generate case list file
                    def keyword = ""
                    if ("${env.JOB_NAME}" == "SingleQueryExt3.0_203") {
                        keyword = "case=Query"
                    } else {
                        keyword = "case=taosc_insert"
                    }
                    case_file = create_case_list("cases_query_replica3.txt", keyword)
                    def env_case_file = create_env_case_list(case_file)
                    run_cases(env_case_file, m_config_file, 1200, "", env.REPLICAS, env.QUERY_POLICY)
                }
            }
        }
        stage('run case') {
            steps {
                script {
                    run_cases(case_file, m_config_file, 7200, "", env.REPLICAS, env.QUERY_POLICY)
                }
            }
        }
        stage('collect log') {
            steps {
                script {
                    sh '''
                        echo "collecting log files ..."
                        cd $TESTNG_ROOT/scripts
                        ./collect_log.sh -k ${JOB_NAME} -d ${WORK_DIR}/testnglog/''' + date_tag + '''
                    '''
                }
            }
        }
    }
    /*post {
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
                                        <b><font color="#0B610B"><font size="6">稳定性测试3.0构建信息</font></font></b>
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
                                        <b><font color="#0B610B"><font size="6">稳定性测试3.0构建信息</font></font></b>
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
    }*/
}