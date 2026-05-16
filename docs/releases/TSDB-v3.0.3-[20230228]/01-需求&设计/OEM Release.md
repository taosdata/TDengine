# OEM Release

OEM release script example:
```bash {wrap}
 // path: TDinternal/enterprise/packaging
 ./new_ver_release.sh -b 3.0 -c x64 -n 3.0.4.0 -l full -v cluster -V stable -d no -N PowerDB -P power -M support@powerdb.com
```


@任新胜 @Shuduo Sang We should add a step by step guide for delivery group to build a OEM version by themselves. QA will first verify this guide. If they successfully build a OEM package without any help from you, then it's acceptable and can be handed over to delivery team to use. In future, we don't deliver OEM directly, but maintain this framework.

### 1. Taosd package

#### 1.1 Variable description

1. Git repo branch name：{branch-name}
2. Release version number：{release-version}   e.g. 3.0.2.4

#### 1.2 Parameter description

Note: Case-sensitive, when actually publishing, the parameters should be replaced according to the actual situation
```shell
-b   :  base version number 3.0
-n   :  release version number
-c   :  cpu type (x64  arm)
-v   :  release version type，must be "cluster" when OEM
-N   :  company name: TDengine(PowerDB)
-P   :  program(client) name: taos(power)
-M   :  support email
```

#### 1.3 Windows

```shell
cd C:\workroom
rm -rf TDinternal
git clone -b {branch-name} --depth=1 https://github.com/taosdata/TDinternal.git
cd TDinternal/enterprise/packaging

new_release.bat -b 3.0 -c x64 -n {release-version} -v cluster -N PowerDB -P power -M suppoert@power.com
ls ../release
```

#### 1.4 Linux

x64：192.168.0.24  
```shell
cd /home/ubuntu/workroom/jenkins/3.0
rm -rf TDinternal
git clone -b {branch-name} --depth=1 https://github.com/taosdata/TDinternal.git
cd TDinternal/enterprise/packaging
./new_ver_release.sh -b 3.0 -c x64 -n {release-version} -v cluster -N PowerDB -P power -M suppoert@power.com
ls /nas/TDengine/v3.0.2.4/enterprise 
```

arm：192.168.1.202 
```shell
cd /home/ubuntu/workroom/jenkins/3.0
rm -rf TDinternal
git clone -b {branch-name} --depth=1 https://github.com/taosdata/TDinternal.git
cd TDinternal/enterprise/packaging
./new_ver_release.sh -b 3.0 -c arm64 -n {release-version} -v cluster -N PowerDB -P power -M suppoert@power.com 
ls /nas/TDengine/v3.0.2.4/enterprise 

```
