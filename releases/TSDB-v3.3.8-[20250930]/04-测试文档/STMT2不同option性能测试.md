# STMT2不同option性能测试

总量100w

表数：1
每批行数：1000000
写入次数：1
option={0, true, true, NULL, NULL}
stmt2-bind Time used: 0.235950 seconds
stmt2-exec Time used: 2.641957 seconds
option={0, false, false, NULL, NULL}
stmt2-bind Time used: 0.417466 seconds
stmt2-exec Time used: 2.550315 seconds

表数：1
每批行数：100000
写入次数：10
option={0, true, true, NULL, NULL}
stmt2-bind Time used: 0.207564 seconds
stmt2-exec Time used: 2.786401 seconds
option={0, false, false, NULL, NULL}
stmt2-bind Time used: 0.102280 seconds
stmt2-exec Time used: 2.993977 seconds

表数：1
每批行数：30000
写入次数：33
option={0, true, true, NULL, NULL}
stmt2-bind Time used: 0.202336 seconds
stmt2-exec Time used: 2.443681 seconds
option={0, false, false, NULL, NULL}
stmt2-bind Time used: 0.090192 seconds
stmt2-exec Time used: 2.156859 seconds

表数：1
每批行数：20000
写入次数：50
option={0, true, true, NULL, NULL}
stmt2-bind Time used: 0.191577 seconds
stmt2-exec Time used: 2.349002 seconds
option={0, false, false, NULL, NULL}
stmt2-bind Time used: 0.086592 seconds
stmt2-exec Time used: 2.226626 seconds
