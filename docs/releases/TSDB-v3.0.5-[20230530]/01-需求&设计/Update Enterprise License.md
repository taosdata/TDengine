# Update Enterprise License

TD-23362

@徐开礼Please Please prepare user manual here. The requirement is to allow user to update both TDengine license and connector license. The mechanism and user manual should be same.

## 1. ~~Status: Ready for review~~

Refer to [Enterprise Licensing](https://taosdata.feishu.cn/wiki/wikcnCZ2U4iFBgZehHW28vd7s9b)

## 2. ~~Backgrounds~~

- ~~Currently, the update of enterprise license is implemented by updating ~~`~~activeCode~~`~~ in taos.cfg manually.~~
- ~~SQL command or ~~~~API~~~~ in taosc should be supported to update the enterprise license.~~

## 3. ~~Reference~~

- ~~JIRA： ~~[~~TD-23362~~](https://jira.taosdata.com:18080/browse/TD-23362)
- [Connector Licensing](https://taosdata.feishu.cn/wiki/wikcnCZ2U4iFBgZehHW28vd7s9b)~~ ~~

## 4. ~~Show ~~

### 4.1 ~~3.1 ~~~~show activeCodes;~~

- ~~Display the raw ~~`~~activeCode~~`~~ in the cluster.~~
```sql
taos> show activeCodes\G;
*************************** 1.row ***************************
     dnodeId: 1
c_activeCode: tP+2soIXpPxJWl7OxrPZ2ElaXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdms3o7P9CUpQk=
*************************** 2.row ***************************
     dnodeId: 2
c_activeCode: tP+2soIXpPxJWl7OxrPZ2ElaXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdms3o7P9CUpQk=
Query OK, 2 row(s) in set (0.001483s)
```

### 4.2 ~~3.2 ~~~~show c_activeCodes~~~~;~~

- ~~Display the raw ~~`~~c_activeCode~~`~~ ~~~~in the cluster.~~
```sql
taos> show c_activeCodes\G;
*************************** 1.row ***************************
     dnodeId: 1
cActiveCode: tP+2soIXpPxJWl7OxrPZ2ElaXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdms3o7P9CUpQk=
*************************** 2.row ***************************
     dnodeId: 2
cActiveCode: tP+2soIXpPxJWl7OxrPZ2ElaXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdms3o7P9CUpQk=
Query OK, 2 row(s) in set (0.001483s)
```

## 5. ~~Updating~~

- ~~Update the active code using the current SQL command ~~`~~ALTER DNODE dnode_id dnode_option~~`~~ and ~~`~~ALTER ALL DNODES dnode_option~~`~~ by extending the ~~`~~dnode_option~~`
```cpp
'activeCode' value  // active code for TDengine  
'cActiveCode' value // active code for connectors
```

- ~~Add an ~~~~API~~~~ in taos.h to update the active code for apps(e.g. TDengine, taos connectors)~~
```sql
/**
  type:    0 TDengine, 1 connectors
  dnodeId: -1 means all dnodes, positive integer: specific dnode.
*/
DLL_EXPORT int  taos_set_activeCode(TAOS *taos, int type, int dnodeId, const char* activeCode);
```

- ~~如果原集群的节点，均被新的服务器逐步替换掉，新的服务器不会自动设置 c_activeCode，需要人工运维(e.g. 手工或者通过工具)。如果不进行人工运维，则相当于一个没有任何授权信息的集群。~~
- ~~增减副本不会影响 c_activeCode 在 dnode/taos.cfg 中的分布。~~
