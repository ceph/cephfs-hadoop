#!/bin/bash

service ssh start

pushd hadoop-2.6.4

sed -i.old '1s;^;JAVA_HOME=/usr/lib/jvm/default-java\n;' etc/hadoop/hadoop-env.sh

cat << EOF > etc/hadoop/core-site.xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

cat << EOF > etc/hadoop/hdfs-site.xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF

cat << EOF > etc/hadoop/mapred-site.xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

cat << EOF > etc/hadoop/yarn-site.xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF

bin/hdfs namenode -format
sbin/start-dfs.sh
sbin/start-yarn.sh

