---
sidebar_label: User Groups
title: User Groups
description: This document describes how to add and delete user groups in TDengine Cloud and how to assign roles and resources to them.
---

In the **User Groups** tab, TDengine Cloud list all user groups in the current organization. You can add new user groups, disable the specific user group and delete it. Also you can assign or unassign any role of the organization to the user group on the resources including organization, instances or databases. If you assign the organization level role **Instance Admin** to the specified user group, the all users in the user group will be administrator of all your instances.

## Add New User Group

You can click **Add New Group** button on the right top of the organization list to open the **Add New Group** dialog. In the opened dialog, you first input name of the new user group and also you can select the permission template of the existed user group. If you do so, the selected user's permissions will be copied to the new user group in the **Roles** part of the dialog. In the **Users** selection, you can add multiple users added to this organization, then these users will be added to the user group. In the last part **Roles**, you can check or uncheck the roles of the specific resources including organization, instances and databases. At the last, you can click **Create** button to submit the request after inputting the verification code from your mail notification, and the new user group will be added to the user group list.

You can also suspend the permissions of an added user group or remove it from the organization in the **Operation** area of the group. Clicking on the **Resources** link in the row where the user group is located brings up a list of the permissions assigned to specific resources for the user group.
