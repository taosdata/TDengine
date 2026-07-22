from new_test_framework.utils import clusterComCheck, sc, tdLog, tdSql, tdStream


class TestSnodeRunnerParallelismFailover:
    NODE_COUNT = 6

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_runner_parallelism_failover(self):
        """Runner parallelism snapshot survives mnode leader failover.

        1. Deploy a stream with Deploy 3, then alter all nodes to 8.
        2. Switch the mnode leader and verify the live task snapshot stays at 3.
        3. Fully redeploy the stream at 8, then alter all nodes back to 3.
        4. Switch the mnode leader again and verify the snapshot stays at 8.

        Catalog:
            - Streams:Snode

        Since: v3.3.3.7

        Labels: common,ci,integration,functional
        Feishu: None

        History:
            - 2026-07-16 GPT-5 Created
        """
        clusterComCheck.checkDnodes(self.NODE_COUNT)
        for dnode_id in range(4, 7):
            tdSql.execute(f"create snode on dnode {dnode_id}")
        tdSql.checkResultsByFunc(
            sql="show snodes",
            func=lambda: tdSql.getRows() == 3,
            retry=60,
        )

        tdSql.prepare(dbname="runner_failover", drop=True, vgroups=1)
        tdSql.execute(
            "create stable runner_source (ts timestamp, v int) tags (t int)"
        )
        tdSql.execute(
            "create table runner_source_0 using runner_source tags (0)"
        )
        self.createRunnerStream("runner_snapshot")
        self.checkRunnerDeployIds("runner_snapshot", 3)
        snapshot3 = self.getRunnerSnapshot("runner_snapshot")

        self.alterParallelismOnAllDnodes(8, 20)
        self.switchLeaderAndCheck("runner_snapshot", 3, snapshot3)

        tdSql.execute("stop stream runner_snapshot")
        tdSql.execute("start stream runner_snapshot")
        tdStream.checkStreamStatus("runner_snapshot")
        self.checkRunnerDeployIds("runner_snapshot", 8)
        snapshot8 = self.getRunnerSnapshot("runner_snapshot")

        self.alterParallelismOnAllDnodes(3, 5)
        self.switchLeaderAndCheck("runner_snapshot", 8, snapshot8)

    def createRunnerStream(self, stream_name):
        tdSql.execute(
            f"create stream {stream_name} interval(10s) sliding(10s) "
            "from runner_source "
            f"into {stream_name}_out as "
            "select _twstart as ts, count(*) as cnt "
            "from runner_source where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus(stream_name)

    def alterParallelismOnAllDnodes(self, deploys, replicas):
        for dnode_id in range(1, self.NODE_COUNT + 1):
            for name, value in (
                ("numOfStreamRunnerDeploys", deploys),
                ("numOfStreamRunnerReplicas", replicas),
            ):
                tdSql.execute(f"alter dnode {dnode_id} '{name} {value}'")
                tdSql.query(
                    f"show dnode {dnode_id} variables like '{name}';"
                )
                tdSql.checkRows(1)
                tdSql.checkData(0, 1, name)
                tdSql.checkData(0, 2, str(value))

    def getMnodeLeader(self):
        tdSql.query(
            "select * from information_schema.ins_mnodes "
            "order by id"
        )
        leaders = [row[0] for row in tdSql.queryResult if row[2] == "leader"]
        if len(leaders) != 1:
            tdLog.exit(f"expected one mnode leader, got {tdSql.queryResult}")
        return leaders[0]

    def waitForThreeMnodes(self):
        def mnodes_ready():
            if tdSql.getRows() != 3:
                return False
            roles = tdSql.getColData(2)
            statuses = tdSql.getColData(3)
            return (
                roles.count("leader") == 1
                and roles.count("follower") == 2
                and all(status == "ready" for status in statuses)
            )

        tdSql.checkResultsByFunc(
            sql=(
                "select * from information_schema.ins_mnodes "
                "order by id"
            ),
            func=mnodes_ready,
            retry=60,
        )

    def switchLeaderAndCheck(self, stream_name, expected_deploys, before):
        old_leader = self.getMnodeLeader()
        stopped = False
        try:
            sc.dnodeForceStop(old_leader)
            stopped = True
            clusterComCheck.check3mnodeoff(old_leader)
            self.waitForStreamRunning(stream_name)
            self.checkRunnerDeployIds(stream_name, expected_deploys)
            after = self.getRunnerSnapshot(stream_name)
            if after != before:
                tdLog.exit(
                    f"runner task snapshot changed across leader switch: "
                    f"before={before}, after={after}"
                )
        finally:
            if stopped:
                self.restartMnode(old_leader)

    def waitForStreamRunning(self, stream_name):
        tdSql.checkResultsByFunc(
            sql=(
                "select status from information_schema.ins_streams "
                f"where stream_name='{stream_name}'"
            ),
            func=lambda: tdSql.getRows() == 1
            and tdSql.getColData(0) == ["Running"],
            retry=60,
        )

    def restartMnode(self, dnode_id):
        sc.dnodeStart(dnode_id)
        clusterComCheck.checkDnodes(self.NODE_COUNT)
        self.waitForThreeMnodes()

    def checkRunnerDeployIds(self, stream_name, expected_deploys):
        tdSql.checkResultsByFunc(
            sql=(
                "select distinct deploy_id "
                "from information_schema.ins_stream_tasks "
                f"where stream_name='{stream_name}' and type='Runner' "
                "order by deploy_id"
            ),
            func=lambda: tdSql.getRows() == expected_deploys
            and tdSql.getColData(0) == list(range(expected_deploys)),
            retry=60,
        )

    def getRunnerSnapshot(self, stream_name):
        tdSql.query(
            "select task_id, deploy_id, task_idx "
            "from information_schema.ins_stream_tasks "
            f"where stream_name='{stream_name}' and type='Runner' "
            "order by deploy_id, task_idx"
        )
        return tuple(tuple(row) for row in tdSql.queryResult)
