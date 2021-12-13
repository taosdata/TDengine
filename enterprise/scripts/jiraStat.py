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

dict = {'excel': '', 'github': '', 'QA': '', 'APP': '', 'Engine1': '', 'Engine2': '', 'Bussiness': '', 'Cloud': '', 'Archit': ''}

testGrpUserList = ['huili', 'cpwu', 'hrchen', 'xyguo', 'jbjia', 'cyjia', 'zwen']
appGrpUserList = ['sdsang', 'bding', 'lbhuo', 'lhhuo', 'xlli', 'xftan', 'yzhao']
engine1GrpUserList = ['wpan', 'glzhao', 'mmwang', 'slzhou', 'xywang', 'yhdeng']
engine2GrpUserList = ['hzcheng', 'cli', 'jcliu', 'mhli']
businessDevGrpUserList = ['mljin', 'yqliu', 'kjduan', 'klxu', 'pxiao', 'xywang', 'zqwang', 'zyyang']
cloudDevGrpUserList = ['sqchang', 'jtlian', 'mszhang', 'gwang', 'yyuan']
architectureGrpUserList = ['slguan', 'hjliao']

develop_total = [0, 0, 0]
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
    GITHUB_sheet = excel.add_sheet("github-statInfo")
    dict['github'] = GITHUB_sheet
    GITHUB_sheet.col(0).width = 4000
    GITHUB_sheet.col(1).width = 4000
    GITHUB_sheet.col(2).width = 4000
    GITHUB_sheet.col(3).width = 4000
    head = ["姓名", " 新增代码行数", "删除代码行数", "提交PR数"]
    for index, value in enumerate(head):
        GITHUB_sheet.write(0, index, value)

    # test grp
    QA_sheet = excel.add_sheet("QA-statInfo")
    dict['QA'] = QA_sheet
    QA_sheet.col(0).width = 3000
    QA_sheet.col(1).width = 3000
    QA_sheet.col(2).width = 3000
    head = ["姓名", "提交的bug", "验证的bug"]
    for index, value in enumerate(head):
        QA_sheet.write(0, index, value)

    # develop grp
    APP_sheet = excel.add_sheet("APP-statInfo")
    dict['APP'] = APP_sheet
    APP_sheet.col(0).width = 3000
    APP_sheet.col(1).width = 4000
    APP_sheet.col(2).width = 4000
    APP_sheet.col(3).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "完成的bug"]
    for index, value in enumerate(head):
        APP_sheet.write(0, index, value)

    Engine1_sheet = excel.add_sheet("Engine1-statInfo")
    dict['Engine1'] = Engine1_sheet
    Engine1_sheet.col(0).width = 3000
    Engine1_sheet.col(1).width = 4000
    Engine1_sheet.col(2).width = 4000
    Engine1_sheet.col(3).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "完成的bug"]
    for index, value in enumerate(head):
        Engine1_sheet.write(0, index, value)

    Engine2_sheet = excel.add_sheet("Engine2-statInfo")
    dict['Engine2'] = Engine2_sheet
    Engine2_sheet.col(0).width = 3000
    Engine2_sheet.col(1).width = 4000
    Engine2_sheet.col(2).width = 4000
    Engine2_sheet.col(3).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "完成的bug"]
    for index, value in enumerate(head):
        Engine2_sheet.write(0, index, value)

    Cloud_sheet = excel.add_sheet("Cloud-statInfo")
    dict['Cloud'] = Cloud_sheet
    Cloud_sheet.col(0).width = 3000
    Cloud_sheet.col(1).width = 4000
    Cloud_sheet.col(2).width = 4000
    Cloud_sheet.col(3).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "完成的bug"]
    for index, value in enumerate(head):
        Cloud_sheet.write(0, index, value)

    Bussiness_sheet = excel.add_sheet("Bussiness-statInfo")
    dict['Bussiness'] = Bussiness_sheet
    Bussiness_sheet.col(0).width = 3000
    Bussiness_sheet.col(1).width = 4000
    Bussiness_sheet.col(2).width = 4000
    Bussiness_sheet.col(3).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "完成的bug"]
    for index, value in enumerate(head):
        Bussiness_sheet.write(0, index, value)

    Archet_sheet = excel.add_sheet("Archet-statInfo")
    dict['Archit'] = Archet_sheet
    Archet_sheet.col(0).width = 3000
    Archet_sheet.col(1).width = 4000
    Archet_sheet.col(2).width = 4000
    Archet_sheet.col(3).width = 4000
    head = ["姓名", "完成的feature", "进行中的feature", "完成的bug"]
    for index, value in enumerate(head):
        Archet_sheet.write(0, index, value)

    sheet = excel.add_sheet("develop-total")
    dict['develop'] = sheet
    sheet.col(0).width = 4000
    sheet.col(1).width = 4000
    sheet.col(2).width = 4000
    sheet.col(3).width = 4000
    head = ["类型", "完成的feature", "进行中的feature", "完成的bug"]
    for index, value in enumerate(head):
        sheet.write(0, index, value)

    return dict

# each develop group get jira info and save into sheet
def develop_grp_save_info(group_name, group_user_list):
    lineIndex = 1
    sheet = dict[group_name]
    completedFeatures_total = 0
    developingFeatures_total = 0
    completedBugs_total = 0
    for index in range(len(group_user_list)):
        completedFeatures = completed_features(jira, group_user_list[index])
        developingFeatures = developing_features(jira, group_user_list[index])
        completedBugs = completed_bugs(jira, group_user_list[index])
        completedFeatures_total = completedFeatures_total + completedFeatures
        developingFeatures_total = developingFeatures_total + developingFeatures
        completedBugs_total = completedBugs_total + completedBugs
        print('userName: %s, completedFeatures:' % group_user_list[index], completedFeatures, ", developingFeatures: ", developingFeatures, ", completedBugs: ", completedBugs)
        sheet.write(lineIndex, 0, group_user_list[index])
        sheet.write(lineIndex, 1, completedFeatures)
        sheet.write(lineIndex, 2, developingFeatures)
        sheet.write(lineIndex, 3, completedBugs)
        lineIndex = lineIndex + 1

    sheet.write(lineIndex, 0, "总计")
    sheet.write(lineIndex, 1, completedFeatures_total)
    sheet.write(lineIndex, 2, developingFeatures_total)
    sheet.write(lineIndex, 3, completedBugs_total)

    develop_total[0] = develop_total[0] + completedFeatures_total
    develop_total[1] = develop_total[1] + developingFeatures_total
    develop_total[2] = develop_total[2] + completedBugs_total
    return

def develop_total_save_info(jiraHandle):
    sheet = dict['develop']
    sheet.write(1, 0, "各组累加")
    sheet.write(1, 1, develop_total[0])
    sheet.write(1, 2, develop_total[1])
    sheet.write(1, 3, develop_total[2])

    completedFeatures_total = 0
    developingFeatures_total = 0
    completedBugs_total = 0

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

    sheet.write(2, 0, "直接查询")
    sheet.write(2, 1, completedFeatures_total)
    sheet.write(2, 2, developingFeatures_total)
    sheet.write(2, 3, completedBugs_total)

    return

# 保存excel文件
def save_file():
    dateString = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
    fileName = 'statInfo-' + dateString + '.xls'
    #print(fileName)
    dict['excel'].save(fileName)
    return

if __name__ == '__main__':
    jira = login_jira('huili','lihui13611161621')
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
    develop_grp_save_info('Engine1', engine1GrpUserList)
    develop_grp_save_info('Engine2', engine2GrpUserList)
    develop_grp_save_info('APP', appGrpUserList)
    develop_grp_save_info('Bussiness', businessDevGrpUserList)
    develop_grp_save_info('Cloud', cloudDevGrpUserList)
    develop_grp_save_info('Archit', architectureGrpUserList)

    ####  total devlop department
    develop_total_save_info(jira)

    #githubInfoFile = "info_2021-09-01-2021-10-31.csv"
    add_github_info_to_file(githubInfoFile)
    save_file()



