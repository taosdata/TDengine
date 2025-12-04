---
sidebar_label: Users
title: Users
description: This document describes how to manage organizations, users, user groups, and roles in TDengine Cloud.
---

TDengine Cloud provides a simple and convenient user management including users, user groups, roles, permissions and resources. TDengine Cloud provides the eight default roles and four level privileges, which includes organization level, instance level, database level and grant privilege level. The default roles are Database Admin, Finance, Data Writer, Data Reader, Developer, Instance Admin, Super Admin and Organization Owner.  You can also create customized roles with TDengine Cloud defined privileges.

Organization owner or Super admin can invite any other users into the organization. Also, he can assign specific roles to users, user groups with specific resources, including instances and databases. At the organization level, instance level or database level, it is so easy for the TDengine data owner to share the data with others through assigning the role including database reader privilege to.  Also, you can update these permissions or remove them easily. Specifically, it is easy to grant developer privileges for an entire instance to quickly give full access to your internal team or grant limited access to specific database resources to specific stakeholders.

You can create multiple organizations, users and user groups. And add the users, user groups in specific organization, instance or database.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
