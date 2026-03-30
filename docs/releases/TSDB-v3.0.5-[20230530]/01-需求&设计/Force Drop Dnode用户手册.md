# Force Drop Dnode用户手册

### 1. 背景

当dnode不在线时，无法drop该dnode

### 2. 解决的问题列表

TD-23346 完成 drop dnode xx force；命令
TD-20044 Force drop dnode 时遗留的几个问题
TD-20044该问题的表现是，vnode单副本的场景下，force drop dnode后，select from stb会报 table not exist的错误。

### 3. 新增命令

```sql {wrap}
drop dnode <dnodeid> force; #如果该dnode上存在单副本的vnode，则给出数据将会丢失的报错，并且建议用户使用unsafe drop执行
drop dnode <dnodeid> unsafe; #功能和force drop完全一致，只是不会有数据丢失的报错
```


增加unsafe drop的命令目的是，通过命令上的明确区分，给用户一个“强提示”数据丢失的事实，如果不通过不同的命令区分，数据丢失的提示只能通过server日志给出，或者通过force drop的执行结果返回，前者不能被用户注意到，后者是事后提示，不是事前提醒。

两个命令功能上没有差异，差别就在于“强提示”。

### 4. 功能详细描述

- Mnode
当mnode的副本为3时，可以执行force，直接将指定dnode上的mnode从mnode删除。

- Qnode
直接将指定dnode上的qnode从mnode删除。

- Vnode
如果所有这些vgroup中，有一个vgroup是单副本，返回给用户数据丢失的错误，对所有vgroup不做任何drop动作（包括前面的mnode和qnode）。
如果不存在单副本，针对指定的dnode上的所有vgroup：
找另外一个适合dnode，在这个dnode上为该vgroup创建一个新的vnode，并且将旧的vnode从mnode中直接删除，
如果这个vgroup是3副本，创建完vnode后，新vnode会开始从leader上复制数据，复制过程不阻塞写入
如果这个vgroup是单副本，创建完vnode后，不会有数据复制
并且会vgroup所在db上的所有stb在该vnode上重新创建（解决TD-20044）

- 与正常的drop dnode的区别
  - 单副本vgroup，创建完新的vnode，正常的drop dnode会有数据复制，并且这个数据复制是阻塞写入的（这个阻塞后面可以优化）
  - 针对待删除的dnode上的mnode，qnode，vnode对象，不会向该dnode发送删除对象的消息，仅仅是将mnode上的记录删除

### 5. 功能限制

1.mnode只有两个副本，一个副本offline，此时无法执行force/unsafe drop。并且此时集群无法进行任何其他会导致往mnode写入数据的操作，force drop也要向mnode写入数据，所以这时无法执行force drop。该场景的唯一解决办法是恢复offline节点。
2.在单副本的情况下，force drop恢复了新vnode上的stb，但是没有恢复table，所以向这个vnode上以前存在的table上写入数据，会报错。
