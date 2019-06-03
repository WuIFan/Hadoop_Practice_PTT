# Hadoop_Practice_PTT

## Command

### start/stop env

cd $HADOOP_HOME/sbin  
./start-dfs.sh  
./start-yarn.sh  
./stop-dfs.sh  
./stop-yarn.sh  

### put data

hadoop fs -mkdir -p /tmp/ptt_target  
hadoop fs -put dateAndTitle2.txt /tmp/ptt_target  

### compile

hadoop com.sun.tools.javac.Main ptt.java  
jar cf hwcs.jar ptt*.class  
jar cf hwcs.jar ptt*.class -C dic/ .  
hadoop jar hwcs.jar ptt  /tmp/ptt_target /tmp/ptt_result  

### result

hadoop fs -rmr /tmp/ptt_result  
hadoop fs -cat /tmp/ptt_result/part-r-00000  
hadoop fs -get /tmp/ptt_sort/part-r-00000  

## Hadoop xml setting  

### core-site  

```xml
<configuration>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/usr/local/Cellar/hadoop/hdfs/tmp</value>
    <description>A base for other temporary directories</description>
  </property>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

### mapred-site.xml  

```xml
<configuration>
  <property>
    <name>mapred.job.tracker</name>
    <value>localhost:9010</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

### hdfs-site.xml

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

### yarn-site.xml

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
  <name>yarn.app.mapreduce.am.env</name>
  <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
  <name>mapreduce.map.env</name>
  <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
  <name>mapreduce.reduce.env</name>
  <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
</configuration>
```

### hadoop-env.sh

```sh
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home
export HADOOP_CONF_DIR=/usr/local/Cellar/hadoop/3.1.2/libexec/etc/hadoop
export HADOOP_OS_TYPE=${HADOOP_OS_TYPE:-$(uname -s)}
export HADOOP_CLASSPATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/lib/tools.jar
```

### bash_profile  

export HADOOP_HOME=/usr/local/Cellar/hadoop/3.1.2/libexec  
export PATH=$PATH:$HADOOP_HOME/bin  

export JAVA_HOME_8=/Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home  
export JAVA_HOME=$JAVA_HOME_8  
alias jdk8='export JAVA_HOME=$JAVA_HOME_8'  

export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar  

## Others  

### 使用外部library

For example : ikanalyzer  
put the IKAnalyzer2012_u6.jar in  
⁨usr⁩ > ⁨local⁩ > ⁨Cellar⁩ >⁨ hadoop⁩ > ⁨3.1.2⁩ > ⁨libexec⁩ > ⁨share⁩ > ⁨hadoop⁩ >  common⁩  > lib
  
and the IKAnalyzer.cfg.xml and other .dic should be jar in your .jar  
