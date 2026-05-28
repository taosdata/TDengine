---
name: tsdb-dev-privilege
description: "TDengine RBAC 权限控制指南 (v3.4.0.0 & Legacy v3.3 兼容)。提供用户/角色管理、细粒度权限控制(数据库、表、列、topic)、旧版语法兼容性等工作流指导。关键词: privilege, RBAC, 权限, 用户管理, 角色, grant, revoke"
metadata:
  author: klxu
  version: 1.0.0
  owner_team: engine
---

# TDengine Privilege Control Skill (v3.4.0.0 & Legacy Compatibility)

## Purpose

Provide guidance and verified workflows for using RBAC privilege control in TDengine v3.4.0.0, including compatibility with legacy v3.3 syntax.

## Scope

- User and role management
- Fine-grained privilege control (database, table, column, topic)
- Legacy syntax compatibility
- Privilege query and troubleshooting

## Primary Workflow (v3.4.0.0 RBAC)

1. **User & Role Management**
   - Create user: `create user <username> pass '<password>'`
   - Create role: `create role <role_name>`
   - Grant role to user: `grant role <role_name> to <username>`
   - Revoke role: `revoke role <role_name> from <username>`

2. **Privilege Granting**
   - Grant system privilege: `grant create database to <username>`
   - Grant database privilege: `grant create table on database <db> to <username>`
   - Grant column privilege: `grant select(c0,c1),insert(ts,c0),delete on table <db>.<table> with <condition> to <username>`
   - Grant topic privilege: `grant subscribe on topic <db>.<topic> to <username>`

3. **Privilege Query & Management**
   - Show role privileges: `show role privileges`
   - Show user privileges: `show user privileges`
   - Revoke privilege: `revoke <privilege> on <object> from <username/role>`

## Legacy Syntax Compatibility (v3.3)

1. **Enable/Disable Legacy Syntax**
   - Enable: `alter all dnodes 'enableGrantLegacySyntax 1'`
   - Disable: `alter all dnodes 'enableGrantLegacySyntax 0'`

2. **Legacy Grant/Revoke Examples**
   - Grant all privileges: `grant all on <db>.* to <username>`
   - Grant read/write: `grant read,write on <db>.* to <username>`
   - Revoke privileges: `revoke all on <db> from <username>`

3. **Notes**
   - Privilege count and scope differ between legacy and RBAC modes.
   - Always check `enableGrantLegacySyntax` before using legacy grammar.

## Troubleshooting

- Use `show user privileges` and `show role privileges` statements to verify privilege assignments.
- Test privilege effects and error scenarios for both RBAC and legacy modes.
- Refer to test_priv_rbac.py for detailed test cases and expected behaviors.

## Example Workflow

1. Create users and roles.
2. Grant privileges using RBAC or legacy syntax as required.
3. Query privileges to verify.
4. Switch legacy mode if compatibility is needed.
5. Test privilege enforcement and error handling.

---

For detailed examples, see test_priv_rbac.py and adjust commands as needed for your environment.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-privilege version=1.0.0 author=klxu`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
