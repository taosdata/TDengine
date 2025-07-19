from test_idmp_tobacco import TestIdmpScene


class TestIdmpPhotovoltaic:
    def test_pv(self):
        """
        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb#share-Ygqld907hoMESmx04GBcRlaVnZz
        1. 测试 AI 推荐生成的分析, 创建 Stream, 验证流的正确性
        2. 测试不同的触发类型
        1. 滑动窗口: 每 n 分钟计算一次 m 小时内的聚合
        2. 事件窗口: field 从 start_condition 开始，到 stop_condition 结束
        3. 会话窗口: 超过 n 分钟没有上报数据
        4. 计数窗口: 连续 n 次采集数据
        3. 不同类型的聚合函数：
        1. AVG: 平均值
        2. LAST: 最新值
        3. SUM: 求和
        4. MAX: 最大值
        Catalog:
            - Streams:UseCases
        Since: v3.3.6.14
        Labels: common,ci
        Jira:
            - https://jira.taosdata.com:18080/browse/TD-36783
        History:
            - 2025-7-18 zyyang90 Created
        """
        pv = TestIdmpScene()
        pv.init(
            "photovoltaic",
            "idmp_sample_pv",
            "idmp",
            "cases/13-StreamProcessing/20-UseCase/pv_data/idmp_sample_pv",
            "cases/13-StreamProcessing/20-UseCase/pv_data/idmp/vstb.sql",
            "cases/13-StreamProcessing/20-UseCase/pv_data/idmp/vtb.sql",
            "cases/13-StreamProcessing/20-UseCase/pv_data/idmp/stream.json",
        )
        pv.stream_ids = [1, 2, 3, 4, 5, 6, 7]
        pv.run()
