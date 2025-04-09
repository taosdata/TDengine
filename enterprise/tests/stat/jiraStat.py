# -*- coding: utf-8 -*-
import datetime

from jira import JIRA
import re
import xlwt
import time
import csv

################ global variables ################
startDate = '2021/12/05 00:00'
endDate = '2021/12/12 00:00'

githubInfoFile = "info_2021-12-05_2021-12-11-new.txt"

dict = {'excel': '', 'github': '', 'QA': '', 'APP': '', 'Engine': '', 'Query': '', 'Bussiness': '', 'Cloud': ''}

testGrpUserList = ['huili', 'cpwu', 'hrchen', 'xyguo', 'jbjia', 'cyjia', 'zwen']
appGrpUserList = ['sdsang', 'bding', 'lbhuo', 'lhhuo', 'xlli', 'xftan', 'yzhao']

engineGrpUserList = ['slguan', 'hzcheng', 'wpan', 'yhdeng', 'mhli', 'xywang', 'jcliu', 'hjliao']
queryGrpUserList = ['glzhao', 'mmwang', 'slzhou']

businessDevGrpUserList = ['mljin', 'yqliu', 'kjduan', 'klxu', 'pxiao', 'xywang', 'zqwang', 'zyyang']
cloudDevGrpUserList = ['sqchang', 'jtlian', 'mszhang', 'yyuan']

develop_total = [0, 0, 0, 0]
fixBugTime = [0, 0]  # first is total bug number, second is total time
##################################################

# login jira
def login_jira(username,password):
    jira = JIRA("https://jira.taosdata.com:18080",basic_auth=(username,password))
    return jira

def created_bugs(jiraHandle, username):
    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND reporter = " + username + " AND (type = RD-Defect OR type = Bug) AND createdDate >= \"" + startDate + "\" AND createdDate < \"" + endDate + "\" ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    #print(filter)
    #print("user:", username, ", create bugs: ", len(issue))
    return len(issue)

def verified_bugs(jiraHandle, username):
    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND assignee = " + username + " AND (type = RD-Defect OR type = Bug) AND status = DONE AND updatedDate >= \"" + startDate + "\" AND updatedDate < \"" + endDate + "\" AND (status CHANGED FROM \"TESTING\" TO \"DONE\" OR status CHANGED FROM \"VERIFYING\" TO \"DONE\") ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    #print(filter)
    #print("user:", username, ", create bugs: ", len(issue))
    return len(issue)

def completed_features(jiraHandle, username):
    #filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND Developer  = " + username + " AND type = RD-Feature AND updatedDate >= \"" + startDate + "\" AND updatedDate < \"" + endDate + "\" AND ((status = DONE AND status CHANGED FROM \"TESTING\" TO \"DONE\") OR (status = TESTING AND status CHANGED FROM \"DEVELOPING\" TO \"TESTING\")) ORDER BY created DESC"
    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND Developer  = " + username + " AND type = RD-Feature AND updatedDate >= \"" + startDate + "\" AND updatedDate < \"" + endDate + "\" AND (status CHANGED FROM  \"DEVELOPING\" TO \"TESTING\") ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    #print(filter)
    #print("user:", username, ", completed features: ", len(issue))
    return len(issue)

def developing_features(jiraHandle, username):
    #filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND Developer = " + username + " AND type = RD-Feature AND updatedDate >= \"" + startDate + "\" AND updatedDate < \"" + endDate + "\" AND ((status = DONE AND status CHANGED FROM \"TESTING\" TO \"DONE\") OR (status = TESTING AND status CHANGED FROM \"DEVELOPING\" TO \"TESTING\")) ORDER BY created DESC"
    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND Developer = " + username + " AND type = RD-Feature AND (status != DONE AND status != NEW AND status != CANCELED AND status != CANCEL ) ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    #print(filter)
    #print("user:", username, ", developing featurens: ", len(issue))
    return len(issue)

def completed_bugs(jiraHandle, username):
    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND Developer  = " + username + " AND (type = RD-Defect OR type = Bug) AND updatedDate >= \"" + startDate + "\" AND updatedDate < \"" + endDate + "\" AND (status CHANGED FROM \"PROCESSING\" TO \"TESTING\") ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    #print(filter)
    #print("user:", username, ", fixed bugs: ", len(issue))
    return len(issue)

# 生成表格文件
def add_github_info_to_file(csvFileName):
    sheet = dict['github']
    addLine_total = 0
    delLine_total = 0
    pr_total = 0
    lineIndex = 1
    titles = ["name", "add", "del", "pr"]
    with open(csvFileName, 'r') as csvFile:
      reader = csv.reader(csvFile)

      for item in reader:
        #row = {}
        for i in range(len(item)):
          #row[titles[i]] = item[i]
          sheet.write(lineIndex, i, item[i])
          if i == 1:
              addLine_total = addLine_total + int(item[i])
          elif i == 2:
              delLine_total = delLine_total + int(item[i])
          elif i == 3:
              pr_total = pr_total + int(item[i])

        lineIndex = lineIndex + 1

    sheet.write(lineIndex, 0, "总计")
    sheet.write(lineIndex, 1, addLine_total)
    sheet.write(lineIndex, 2, delLine_total)
    sheet.write(lineIndex, 3, pr_total)
    return


# 生成表格文件
def create_file():
    # 创建一个excel
    excel = xlwt.Workbook(encoding='utf-8')
    dict['excel'] = excel

    # 添加工作区
    sheet = excel.add_sheet("github-statInfo")
    dict['github'] = sheet
    sheet.col(0).width = 4000
    sheet.col(1).width = 4000
    sheet.col(2).width = 4000
    sheet.col(3).width = 4000
    head = ["姓名", " 新增代码行数", "删除代码行数", "提交PR数"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    # test grp
    sheet = excel.add_sheet("测试组")
    dict['QA'] = sheet
    sheet.col(0).width = 3000
    sheet.col(1).width = 3000
    sheet.col(2).width = 3000
    head = ["姓名", "提交的bug", "验证的bug"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    # develop grp
    sheet = excel.add_sheet("应用组")
    dict['APP'] = sheet
    sheet.col(0).width = 3000
    sheet.col(1).width = 4000
    sheet.col(2).width = 4000
    sheet.col(3).width = 4000
    sheet.col(4).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "解决的bug", "新增的bug"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    sheet = excel.add_sheet("引擎组")
    dict['Engine'] = sheet
    sheet.col(0).width = 3000
    sheet.col(1).width = 4000
    sheet.col(2).width = 4000
    sheet.col(3).width = 4000
    sheet.col(4).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "解决的bug", "新增的bug"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    sheet = excel.add_sheet("查询组")
    dict['Query'] = sheet
    sheet.col(0).width = 3000
    sheet.col(1).width = 4000
    sheet.col(2).width = 4000
    sheet.col(3).width = 4000
    sheet.col(4).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "解决的bug", "新增的bug"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    sheet = excel.add_sheet("云服务组")
    dict['Cloud'] = sheet
    sheet.col(0).width = 3000
    sheet.col(1).width = 4000
    sheet.col(2).width = 4000
    sheet.col(3).width = 4000
    sheet.col(4).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "解决的bug", "新增的bug"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    sheet = excel.add_sheet("业务开发组")
    dict['Bussiness'] = sheet
    sheet.col(0).width = 2000
    sheet.col(1).width = 4000
    sheet.col(2).width = 4000
    sheet.col(3).width = 4000
    sheet.col(4).width = 4000
    sheet.col(5).width = 5500
    sheet.col(6).width = 3000
    head = ["姓名", "完成的feature", "进行中的feature", "解决的bug", "新增的bug", "解决bug平均时长(小时)", "完成的bug"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    sheet = excel.add_sheet("研发汇总")
    dict['develop'] = sheet
    sheet.col(0).width = 4000
    sheet.col(1).width = 4000
    sheet.col(2).width = 4000
    sheet.col(3).width = 4000
    sheet.col(4).width = 4000
    head = ["类型", "完成的feature", "进行中的feature", "解决的bug", "新增的bug"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    return dict

def timeSpent(jiraHandle, lineIndex, group_name, username):
    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND (type = RD-Defect OR type = Bug) AND Developer = \"" + username + "\" AND updatedDate >= \"" + startDate + "\" AND updatedDate < \"" + endDate + "\" AND (status CHANGED FROM \"TESTING\" TO \"DONE\" OR status CHANGED FROM \"VERIFYING\" TO \"DONE\") ORDER BY created DESC"
    issues = jiraHandle.search_issues(filter, maxResults=-1)

    fix_bugs = 0
    fix_bugs_time = 0
    for issue_key in issues:
        currentIssue = jiraHandle.issue(issue_key)
        created_str = currentIssue.fields.created
        created_str2 = created_str[0:10] + ' ' + created_str[11:23]
        created_ts = time.mktime(time.strptime(created_str2, "%Y-%m-%d %H:%M:%S.%f"))
        #print('created_ts: ', created_ts)
        updated_str = currentIssue.fields.updated
        updated_str2 = updated_str[0:10] + ' ' + updated_str[11:23]
        updated_ts = time.mktime(time.strptime(updated_str2, "%Y-%m-%d %H:%M:%S.%f"))
        #print('updated_ts: ', updated_ts)
        fix_bugs += 1
        fix_bugs_time += updated_ts - created_ts

    #print(username, fix_bugs)
    if fix_bugs != 0 :
      avg_time = fix_bugs_time / fix_bugs / 3600
    else :
      avg_time = 0

    #print('avg_time: ', avg_time)
    Bussiness_sheet = dict[group_name]
    Bussiness_sheet.write(lineIndex, 5, int(avg_time))
    Bussiness_sheet.write(lineIndex, 6, fix_bugs)

    fixBugTime[0] += fix_bugs
    fixBugTime[1] += fix_bugs_time

    return

def newBugs(jiraHandle, username):
    # new add bugs
    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND (type = RD-Defect OR type = Bug) AND assignee = \"" + username + "\" AND createdDate >= \"" + startDate + "\" AND createdDate < \"" + endDate + "\" ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    return len(issue)

# each develop group get jira info and save into sheet
def develop_grp_save_info(group_name, group_user_list):
    lineIndex = 1
    sheet = dict[group_name]
    completedFeatures_total = 0
    developingFeatures_total = 0
    completedBugs_total = 0
    addNewBugs_total = 0
    for index in range(len(group_user_list)):
        completedFeatures = completed_features(jira, group_user_list[index])
        developingFeatures = developing_features(jira, group_user_list[index])
        completedBugs = completed_bugs(jira, group_user_list[index])
        addNewBugs = newBugs(jira, group_user_list[index])

        completedFeatures_total = completedFeatures_total + completedFeatures
        developingFeatures_total = developingFeatures_total + developingFeatures
        completedBugs_total = completedBugs_total + completedBugs
        addNewBugs_total = addNewBugs_total + addNewBugs

        print('userName: %s, completedFeatures:' % group_user_list[index], completedFeatures, ", developingFeatures: ", developingFeatures, ", completedBugs: ", completedBugs, ", addNewBugs: ", addNewBugs)
        sheet.write(lineIndex, 0, group_user_list[index])
        sheet.write(lineIndex, 1, completedFeatures)
        sheet.write(lineIndex, 2, developingFeatures)
        sheet.write(lineIndex, 3, completedBugs)
        sheet.write(lineIndex, 4, addNewBugs)
        lineIndex = lineIndex + 1

    sheet.write(lineIndex, 0, "总计")
    sheet.write(lineIndex, 1, completedFeatures_total)
    sheet.write(lineIndex, 2, developingFeatures_total)
    sheet.write(lineIndex, 3, completedBugs_total)
    sheet.write(lineIndex, 4, addNewBugs_total)

    develop_total[0] = develop_total[0] + completedFeatures_total
    develop_total[1] = develop_total[1] + developingFeatures_total
    develop_total[2] = develop_total[2] + completedBugs_total
    develop_total[3] = develop_total[3] + addNewBugs_total
    return

def develop_total_save_info(jiraHandle):
    sheet = dict['develop']
    sheet.write(1, 0, "各组累加")
    sheet.write(1, 1, develop_total[0])
    sheet.write(1, 2, develop_total[1])
    sheet.write(1, 3, develop_total[2])
    sheet.write(1, 4, develop_total[3])

    completedFeatures_total = 0
    developingFeatures_total = 0
    completedBugs_total = 0
    addNewBugs_total = 0

    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND type = RD-Feature AND updatedDate >= \"" + startDate + "\" AND updatedDate < \"" + endDate + "\" AND (status = DONE AND status CHANGED FROM \"TESTING\" TO \"DONE\") ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    # print(filter)
    # print("completedFeatures_total: ", len(issue))
    completedFeatures_total = len(issue)

    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND type = RD-Feature AND (status != DONE AND status != NEW AND status != CANCELED AND status != CANCEL) ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    # print(filter)
    # print("developingFeatures_total: ", len(issue))
    developingFeatures_total = len(issue)

    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND (type = RD-Defect OR type = Bug) AND updatedDate >= \"" + startDate + "\" AND updatedDate < \"" + endDate + "\" AND (status = DONE AND ((status CHANGED FROM \"TESTING\" TO \"DONE\") OR (status CHANGED FROM \"VERIFYING\" TO \"DONE\"))) ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    # print(filter)
    # print("completedBugs_total: ", len(issue))
    completedBugs_total = len(issue)

    # new add bugs
    filter = "(project = \"Taos Development\" OR project = \"Taos Support\") AND (type = RD-Defect OR type = Bug) AND createdDate >= \"" + startDate + "\" AND createdDate < \"" + endDate + "\" ORDER BY created DESC"
    issue = jiraHandle.search_issues(filter, maxResults=-1)
    addNewBugs_total = len(issue)

    sheet.write(2, 0, "直接查询")
    sheet.write(2, 1, completedFeatures_total)
    sheet.write(2, 2, developingFeatures_total)
    sheet.write(2, 3, completedBugs_total)
    sheet.write(2, 4, addNewBugs_total)

    return

# 保存excel文件
def save_file():
    dateString = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
    fileName = 'statInfo-' + dateString + '.xls'
    #print(fileName)
    dict['excel'].save(fileName)
    return

def calcFixBugTime(group_name, group_user_list):
    lineIndex = 1
    for index in range(len(group_user_list)):
        timeSpent(jira, index+1, group_name, group_user_list[index])
        lineIndex += 1

    #print('lineIndex: ', lineIndex)
    Bussiness_sheet = dict[group_name]
    if fixBugTime[0] != 0 :
      avg_time = fixBugTime[1] / fixBugTime[0] / 3600
    else :
        avg_time = 0

    Bussiness_sheet.write(lineIndex, 5, int(avg_time))
    Bussiness_sheet.write(lineIndex, 6, fixBugTime[0])

    return

if __name__ == '__main__':
    jira = login_jira('xxxx','xxxxxx')
    #projects = jira.projects()
    #print(projects)

    print("Statistical date: ", startDate, endDate)

    dict = create_file()

    #### test group
    lineIndex = 1
    sheet = dict['QA']
    createdBugs_total = 0
    verifiedBugs_total = 0
    for index in range(len(testGrpUserList)):
        #print('userName : %s' % testGrpUserList[index])
        createdBugs = created_bugs(jira, testGrpUserList[index])
        verifiedBugs = verified_bugs(jira, testGrpUserList[index])
        createdBugs_total = createdBugs_total + createdBugs
        verifiedBugs_total = verifiedBugs_total + verifiedBugs
        print('userName: %s, createdBugs:' % testGrpUserList[index], createdBugs, ", verifiedBugs: ", verifiedBugs)
        sheet.write(lineIndex, 0, testGrpUserList[index])
        sheet.write(lineIndex, 1, createdBugs)
        sheet.write(lineIndex, 2, verifiedBugs)
        lineIndex = lineIndex + 1

    sheet.write(lineIndex, 0, "总计")
    sheet.write(lineIndex, 1, createdBugs_total)
    sheet.write(lineIndex, 2, verifiedBugs_total)

    #### develop groups
    develop_grp_save_info('Engine', engineGrpUserList)
    develop_grp_save_info('Query', queryGrpUserList)
    develop_grp_save_info('APP', appGrpUserList)
    develop_grp_save_info('Bussiness', businessDevGrpUserList)
    develop_grp_save_info('Cloud', cloudDevGrpUserList)

    ####  total devlop department
    develop_total_save_info(jira)

    calcFixBugTime('Bussiness', businessDevGrpUserList)

    #githubInfoFile = "info_2021-09-01-2021-10-31.csv"
    add_github_info_to_file(githubInfoFile)
    save_file()




