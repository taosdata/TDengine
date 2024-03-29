<center><h1>VNODE Write Processes</h1></center>

## META Operations
META data write operations including:

1. create table
2. drop table
3. alter table

We take create table as an example to figure out the whole process.
```plantuml
@startuml create_table
skinparam sequenceMessageAlign center
skinparam responseMessageBelowArrow true

participant APP as app
box "dnode1"
    participant RPC as rpc
    participant VNODE as vnode
    participant SYNC as sync
end box

box "dnode2"
    participant SYNC as sync2
    participant VNODE as vnode2
end box

box "dnode3"
    participant SYNC as sync3
    participant VNODE as vnode3
end box

' APP send request to dnode and RPC in dnode recv the request
app ->rpc: create table req

' RPC call vnodeProcessReq() function to process the request
rpc -> vnode: vnodeProcessReq
note right
callback function 
run in RPC module 
threads. The function
only puts the request
to a vnode queue.
end note

' VNODE call vnodeProcessReqs() function to integrate requests and process as a whole
vnode -> vnode: vnodeProcessReqs()
note right
integrate reqs and 
process as a whole
end note


' sync the request to other nodes
vnode -> sync: syncProcessReqs()

' make request persistent
' sync -->vnode: walWrite()\n(callback function)

' replicate requests to other DNODES
sync -> sync2: replication req
sync -> sync3: replication req
sync2 -> vnode2: walWrite()\n(callback function)
sync2 --> sync: replication rsp\n(confirm)
sync3 -> vnode3: walWrite()\n(callback function)

sync3 --> sync: replication rsp\n(confirm)

' send apply request
sync -> sync2: apply req
sync -> sync3: apply req

' vnode apply
sync2 -> vnode2: vnodeApplyReqs()
sync3 -> vnode3: vnodeApplyReqs()

' call apply request
sync --> vnode: vnodeApplyReqs()\n(callback function)

' send response
vnode --> rpc: rpcSendRsp()

' dnode send response to APP
rpc --> app: create table rsp
@enduml
```

## Time-series data Operations
There are only one operations for time-series data: data insert. We will figure out the whole process.

```plantuml
@startuml create_table
skinparam sequenceMessageAlign center
skinparam responseMessageBelowArrow true

participant APP as app
box "dnode1"
    participant RPC as rpc
    participant VNODE as vnode
    participant SYNC as sync
end box

box "dnode2"
    participant SYNC as sync2
    participant VNODE as vnode2
end box

box "dnode3"
    participant SYNC as sync3
    participant VNODE as vnode3
end box

' APP send request to dnode and RPC in dnode recv the request
app ->rpc: insert data req

' RPC call vnodeProcessReq() function to process the request
rpc -> vnode: vnodeProcessReq
note right
callback function 
run in RPC module 
threads. The function
only puts the request
to a vnode queue.
end note

' VNODE call vnodeProcessReqs() function to integrate requests and process as a whole
vnode -> vnode: vnodeProcessReqs()
note right
integrate reqs and 
process as a whole
end note


' sync the request to other nodes
vnode -> sync: syncProcessReqs()

' ' make request persistent
' ' sync -->vnode: walWrite()\n(callback function)

' ' replicate requests to other DNODES
sync -> sync2: replication req
sync -> sync3: replication req

' vnode apply
sync2 -> vnode2: vnodeApplyReqs()
sync3 -> vnode3: vnodeApplyReqs()

' call apply request
sync --> vnode: vnodeApplyReqs()\n(callback function)

' send response
vnode --> rpc: rpcSendRsp()

' dnode send response to APP
rpc --> app: insert data rsp
@enduml
```

## vnodeProcessReqs()
```plantuml
@startuml vnodeProcessReqs()
participant VNODE as v
participant SYNC as s

group vnodeProcessReqs()
    ' Group requests and get a request batch to process as a whole
    v -> v: vnodeGetReqsFromQueue()
    note right
    integrate all write
    requests as a batch
    to process as a whole
    end note

    ' VNODE call syncProcessReqs() function to process the batch request
    v -> s: syncProcessReqs()

    group syncProcessReqs()
        ' Check if current node is leader
        alt not leader
            return NOT_LEADER
        end

        s -> s: syncAppendReqsToLogStore()
        group syncAppendReqsToLogStore()
            s -> v: walWrite()
            note right
            There must be a 
            callback function 
            provided by VNODE 
            to persist the 
            requests in WAL
            end note

            alt (no unapplied reqs) AND (only one node OR no meta requests)
                s -> v: vnodeApplyReqs()
                note right
                just use the woker
                thread to apply
                the requests. This
                is a callback function
                provided by VNODE
                end note
            else other cases need to wait response
                s -> s:
                note right
                save the requests in log store
                and wait for confirmation or
                other cases
                end note

                s ->]: send replication requests
                s ->]: send replication requests
            end
        end
    end
end
@enduml
```

<!-- ## syncProcessReplicationReq()
```plantuml
@startuml syncProcessReplicationReq
participant SYNC as s
participant VNODE as v

-> s: replication request
s -> s:
note right
process the request
to get the request
batch
end note

s -> s: syncAppendReqToLogStore()

s -> v: walWrite()

alt has meta req
    <- s: confirmation
else
    s -> v: vnodeApplyReqs()
end

@enduml -->
<!-- ``` -->

## vnodeApplyReqs()
The function *vnodeApplyReqs()* is the actual function running by a vnode to process the requests.
```plantuml
@startuml vnodeApplyReqs()
skinparam sequenceMessageAlign left
skinparam responseMessageBelowArrow true

participant VNODE as vnode
participant TQ as tq
participant TSDB as tsdb
participant META as meta

group vnodeApplyReqs()
    autonumber
    loop nReqs
        ' Copy request message to vnode buffer pool
        vnode -> vnode: vnodeCopyReq()
        note right
        copy request to 
        vnode buffer pool
        end note

        vnode -> tq: tqPush()
        note right
        push the request 
        to TQ so consumers 
        can consume
        end note
        alt META_REQ
            autonumber 3
            vnode -> meta: metaApplyReq()
        else TS_REQ
            autonumber 3
            vnode -> tsdb: tsdbApplyReq()
        end

    end

    ' Check if need to commit
    alt vnode buffer pool is full
        group vnodeCommit()
            autonumber 4.1
            vnode -> tq: tqCommit()
            note right
            tqCommit may renew wal
            end note
            vnode -> meta: metaCommit();
            note right
            commit meta data
            end note
            vnode -> tsdb: tsdbCommit();
            note right
            commit time-series data
            end note
        end
    end
end
@enduml
```
<!-- meta操作：建表，删表，改表（队队列/同步）
数据写入
快照文件与sync的结合
vnodeOpen（）
vnodeClose（）
sync.h  -->
