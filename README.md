# Hadoop_Practice_PTT


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