# TDengine 3.3.0.0 Release

## 1. Release Date:  2024/4/30

### 1.1 引擎

1. **时序数据 Join**  (**有风险**)  
   - Functional Spec: [Join 功能](https://taosdata.feishu.cn/wiki/NQqNwJirriwpmpkaDbrc4sb6ncg)
   - Test Report: 
2. **复合主键（重复时间戳）**（**有风险**）
   - Functional Spec: [复合主键](https://taosdata.feishu.cn/wiki/OLQjwCpQhiRFE3kS8Uvc3sornRb)
   - Test Report: 
3. **TDengine 双活 （企业版）**(Client: Java Only)
   - Functional Spec: [TDengine 双活 ](https://taosdata.feishu.cn/wiki/E9NmwBfIbiTA5bkq8kScFX0yn8c)
   - Test Report: 
4. TDengine 双副本（+Arbitrator）（企业版）
   - Functional Spec: [TDengine 双副本](https://taosdata.feishu.cn/wiki/CTSLwLgcLitcGlkAh21cnY1ln0g)
   - Test Report:
5. TSMA 
   - Functional Spec: [TSMA 功能](https://taosdata.feishu.cn/wiki/WpVfwsKjeilOtckp3U2cIaz0nef)
   - Test Report: 
6. **新数据类型：Blob**  （**有风险**） 技术方案：
   - Functional Spec: [BLOB时序存储 - 功能说明](https://taosdata.feishu.cn/wiki/BPCJwmWDoi5aZBknjzrcR1N9ndi)
   - Design: [BLOB时序存储：文件及格式概要](https://taosdata.feishu.cn/wiki/NC5pw3cVhizpTVkAQAscKm2enoe)
   - Test Report: 
7. **存储压缩增强**（更多压缩算法且可配置）
   - Functional Spec: [TDengine 压缩增强](https://taosdata.feishu.cn/wiki/St4WwSX5Ei3VfMk3yMUcv2DMnMh)
   - Test Report: 
8. **S3 完全可用** （企业版）
   - Functional Spec: 
   - Test Report:
9. Count Window (查询）
   - Functional Spec: [批查询 count window](https://taosdata.feishu.cn/wiki/T6mLwjOJBiHFKIk86EOck833nSg)
   - Test Report: 
10. **数据库加密（基础版）（企业版）** 
   - Functional Spec: 
   - Test Report: 

### 1.2 taosX

1. Transformer  （企业版）
   - 行协议
   - 2.6 支持修改表名
2. **数据接入 （企业版）**
   - Oracle 时序数据 -> 3.0
      - Functional Spec: 
      - Test Report: 
   - MySQL -> 3.0
      - Functional Spec: 
      - Test Report: 
   - PostgreSql -> 3.0
      - Functional Spec: 
      - Test Report: 
   - ClickHouse -> 3.0
      - Functional Spec: 
      - Test Report: 
3. 数据复制：支持定时任务
   - Functional Spec: 
   - Test Report: 
4. 优化数据备份和恢复部分的产品体验
   - Functional Spec: 
   - Test Report: 
5. **Explorer 开源版**
   - Functional Spec: ** **[taos-explorer 开源版](https://taosdata.feishu.cn/wiki/DItswBcHciHfpPkJJIXcjbw5n0D)** **
   - Test Report: 
6. taos shell 加上开源版提示（文本与 Explorer 保持一致）
   - Functional Spec: 
   - Test Report:
