1. 准备Java环境
   1) 从Oracle官方站点下载并安装 JDK(版本号:Java SE 8u131或者更高版本)
   2) 输出系统环境变量JAVA_HOME，例如你的JDK安装目录是/usr/local/jdk1.8_131，输出JAVA_HOME如下所示：
      export  JAVA_HOME=/usr/local/jdk1.8_131
   3) 输出JDK下的bin目录到系统环境变量PATH中，如下所示：
      export  PATH=$PATH:$JAVA_HOME/bin
   4) 执行java -version命令确认得到下面的类似信息，说明你已经配置好了你的Java环境
      java version "1.8.0_131"
      Java(TM) SE Runtime Environment (build 1.8.0_131-b11)
      Java HotSpot(TM) 64-Bit Server VM (build 25.131-b11, mixed mode)

2. 准备Maven环境
   1）从Apache官方站点上下载Maven压缩包（版本号：3.5）
   2）解压缩maven压缩包在任意路径
   3）输出Maven安装目录下的bin目录到系统变量PATH中，假设你的maven安装路径为/usr/local/apache-maven-3.5.0, 则执行下面的命令：
      export PATH=$PATH:/usr/local/apache-maven-3.5.0/bin
   4) 在命令窗口中执行mvn -version命令，如果看到下面的类似输出信息，说明你的maven已经配置成功。
      Apache Maven 3.5.0 (ff8f5e7444045639af65f6095c62210b5713f426; 2017-04-04T03:39:06+08:00)
      Maven home: /usr/local/apache-maven-3.5.0

3. 编译JDBC
   1）在taosdata目录下执行make clean既可以清除所有JDBC生成文件
   2）在taosdata目录下执行make命令，对JDBC Driver进行编译，并将生成的jar文件放置在build/lib目录下
