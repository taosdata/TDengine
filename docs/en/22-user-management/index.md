---
sidebar_label: User Management
title: User Management
description: Use user management to manage the organizations, users, user groups, roles, permissions and resources.
---

TDengine Cloud provides a simple and convinent user management including users, user groups, roles, permissions and resources. TDengine Cloud provides the eight default roles and four level privileges, which includes organization level, instance level, database level and grant privilege level. The default roles are Database Admin, Finance, Data Writer, Data Reader, Developer, Instance Admin, Super Admin and Organization Owner.  You can also create customized roles with TDengine Cloud defined privileges. 

Organization owner or Super admin can invite any other users into the organization. Also, he can assign specific roles to users, user groups with specific resources, including instances and databases. At the organization level, instance level or database level, it is so easy for the TDengine data owner to share the data with others through assigning the role including database reader privilege to.  Also, you can update these permissions or remove them easily. Specifically, it is easy to grant developer privileges for an entire instance to quickly give full access to your internal team or grant limited access to specific database resources to specific stakeholders.

You can create multiple organizations, users and user groups. And add the users, user groups in specific organization, instance or database.

This section introduces the major features and typical use-cases to help you get a high level overview of the whole user management.

## Major Features

The major features are listed below:

1. [Organization Management](./orgs/): Create new organizations, update their name and also can transfer the owner to some one in the organization.
2. [User Mgmt](./users/): Create, update or delete users or user groups. You can also create/edit/delete customized roles.
    - [User](./users/users)

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
