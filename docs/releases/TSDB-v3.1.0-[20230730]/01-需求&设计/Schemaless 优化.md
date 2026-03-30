# Schemaless 优化

1. 自动建表时间列 ts 名优化
   - taos.cfg 增加 smlTsDefaultName 配置（值为字符串），只在client端起作用，配置后，schemaless自动建表的时间列名字可以通过该配置设置。不配置的话，默认为 _ts
2. 自动建表表名包含点号（.）优化。
   - 由于sql建表表名不支持点号（.），所以schemaless也对点号（.）做了处理。taos.cfg 增加 smlDot2Underline 配置（值为bool型 0/1），只在client端起作用，配置后，true的话，schemaless自动建表的表名如果有点号（.），会自动替换为下划线（_），false的话，还为点号（.）。不配置的话，默认为true。
   - 如果配置了 smlChildTableName ，手动指定子表名的话，子表名里有点号（.），同样按照上面的配置逻辑处理。
   - 添加 smlDot2Underline 配置主要是为了兼容已有存在点号（.）的逻辑。
