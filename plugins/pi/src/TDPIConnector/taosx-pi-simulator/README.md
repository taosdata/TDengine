## 说明
taosx-simulator 通过读取当全目录下 /csv/*.csv 文件，为每一个 csv 文件在 PI System 中生成一个 Point 点位。

## 版本说明
### 1.0.0.*
1. 从 csv\*.csv 读取数据，创建相应 PI 点位。只支持默认 double 类型。
2. 创建点位同时会将新建的点位列表写入 Points.csv 文件，方便后续操作，比如拷贝到 PI Connector 进行监测。
3. 创建点位后会将 csv 中的数值，以当前时间按照文件中的时间间隔模拟写入 PI.
4. -d 参数可以从 PI 中 drop point.csv 中出现的数据，同时 drop 在 TDengine 中出现的对应子表。

