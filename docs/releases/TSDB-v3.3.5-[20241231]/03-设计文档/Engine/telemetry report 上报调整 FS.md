# telemetry report 上报调整 FS 

### 1. 背景

  TDengine的上报连接到了tdengine.com, 而且指出是美国地址，在当前形势下，对我们不利，具体见
TD-32951



| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/12/02 | 0.1 | 邓怡豪 |  |

#### 1.1 主要调整

1. 确保一天只上报一次，上报后，链接一定要断开，不能一直连着。
2. 对于企业版，把telemetryReport 默认关闭。 
3. 上报的地址，如果实例是中国的IP，上报到taosdata.com, 如果是海外的IP，上报到tdengine.com
 其中，需求1/2直接在引擎侧调整即可，需求3单靠引擎侧无法直接处理，主要原因是安装 TDengine 的机器没有简单的办法得知自己是否在中国还是中国外，也就无法直接决定是要连taosdata.com还是tdengine.com, 因此，考虑有如下方案： 
   TDengine实例上报到 telemery服务端(假设为A),  A 收到 req 后，根据 IP 来源判断请求来自国内还是国外，并根据此信息来决定生成resp的内容，如果是国内，resp 中填充 taosdata.com, 如果是国外，resp 中填充tdengine.com, TDengine 实例收到 resp后，缓存在本地（假设为addr），之后TDengine 实例通过addr 进行上报。 
 **可能的问题： **A现在部署在哪里，如果还是国外，那么就不可避免的还是需要访问一次国外的地址
