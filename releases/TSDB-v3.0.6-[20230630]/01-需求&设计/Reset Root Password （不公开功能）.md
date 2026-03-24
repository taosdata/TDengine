# Reset Root Password （不公开功能）

@徐开礼 Please prepare user manual first

TS-3134

## 1. Backgrounds

- If the root user forgets the root password,  provide a method to allow the user to reset.
- Mysql/Oracle/TiDB, etc. all have methods to reset the root password.
- Refer: [Reset User Password](https://taosdata.feishu.cn/wiki/wikcnZV1Wws09iwVrMh1S0LrlMh) 

## 2. Configuration

- Add a new configuration parameter, "skipGrant", in taos.cfg. 
```plaintext

## 1 any user can use any password to log in

## 0 only valid user with correct password can log in(default value)

## 3. skipGrant 0

```

## 4. Workflow

If the DBA (normally with root privilege) really forgets the password for the "root" account, he can follow the procedures below to reset:
- Configure "skipGrant" to 1 in taos.cfg for all mnodes
- Reboot all mnodes
- Use user "root" with any password to log in
```cpp
taos -u root
```

- Change the password for user "root"
```sql
alter user root pass 'xxx';
```

- Configure "skipGrant" to 0 in taos.cfg for all mnodes
- Reboot all mnodes
- Use user "root" with correct password to verify
- Use user "root" with wrong password to verify
