# rebuild tag0 

#### 1. 行为和兼容性 

1. 针对之前建立的超级表，由于当时tag0的indexName 不能查询， 因此重建步骤如下： 
         - create index indexName on superTableName(tag0),  
         - drop index indexName
         - create index indexName on superTableName(tag0)
      其中: 步骤1在mnode 创建了indexName,  并不会在vnode 上真正的建立索引（也不需要建立，之前已经存在） 
                步骤2会真正的删除vnode针对tag0 建立的索引。        
1. 新建立的超级表，会给tag0的索引，随机生成一个indexNewName, 生成规则是： tag0的name + 23个byte, 在系统表可以查，也可以按需要drop， 行为和其他列tag 的索引一样

已经测试的项目
   - 针对之前建立的超级表，对其进行重建，查询超级表的数据 
   - 针对新建立的超级表，查询indexs、超级表条件查
   - 通过系统表查询存在的索引 
   - 语法约束和边界测试 
   
  
相关的测试用例
  由于本次的是在只是支持重建tag0的index, 而且默认行为有一定的改变（第一个列的的indexName 会被默认生成，之前没有), 所涉及的测试用例是在现有测试用例上改动而来。
   - [add_index.sim](https://github.com/taosdata/TDengine/pull/22427/commits/0f0d0953cdbed4eaeed80e114cf2de6dccbd9290#diff-2b11fac70a606e8ec865cf56bfe4d65716df884cfdb1fdff27ef5976ac7d8fdf)
   - [sma_and_tag_index.sim](https://github.com/taosdata/TDengine/pull/22427/commits/0f0d0953cdbed4eaeed80e114cf2de6dccbd9290#diff-696364380460247a04af90842f1fc6bc6d4f54fb000544f369d3f8c3296eee8f)
   - [drop_sma.sim](https://github.com/taosdata/TDengine/pull/22427/commits/a576a3b972ff69adde0b88907be7f37afadc7201#diff-34e4ed38cbfe34a42caf51aac93d8a2651e3c8569d3b7c3bb91a125bd3ca3ec5)
   - [show_tag_index.py](https://github.com/taosdata/TDengine/pull/22427/commits/271ecf6beff4dcea8e83b6945406a3050dd2d793#diff-8e3e06fd384044281e06ada8a654b003177a5acf04685ecb74f9f3cfc9e161cc)
   - [tag_index_basic.py](https://github.com/taosdata/TDengine/pull/22427/commits/271ecf6beff4dcea8e83b6945406a3050dd2d793#diff-235170fa7c94e2e9256b16a3b5359877802e52689047febe33ec889c98607824)
