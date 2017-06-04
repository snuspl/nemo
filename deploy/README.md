# Vortex Cluster Deployment Guide

## Install the operating system
* Ubuntu 14.04.4 LTS
* Other OS/versions not tested

## Install base software in each node
* `initialize_fresh_ubuntu.sh`

## Set hostnames
* Assign a name to each node (v-master, v-worker1, v-worker2, ...)
* Set `/etc/hosts` accordingly on v-master and `pscp` the file to all v-workers 
* Set `hostname` of each node using `set_hostname.sh`

## Set up Hadoop
* Set up the conf files(core-site.xml, hdfs-site.xml, yarn-site.xml, slaves, ...) on v-master, and `pscp` them to all v-workers
* hdfs namenode -format (does not always cleanly format... may need to delete the hdfs directory)
* start-yarn.sh && start-dfs.sh 
* For more information, refer to the official Hadoop website

## Commands for copying files
```bash
pscp -h ~/parallel/hostfile /etc/hosts /etc/hosts 
pscp -h ~/parallel/hostfile /home/ubuntu/hadoop/etc/hadoop/core-site.xml /home/ubuntu/hadoop/etc/hadoop/core-site.xml
pscp -h ~/parallel/hostfile /home/ubuntu/hadoop/etc/hadoop/yarn-site.xml /home/ubuntu/hadoop/etc/hadoop/yarn-site.xml
```

## Commands for querying cluster status
```bash
pssh -i -h ~/parallel/hostfile 'echo $HADOOP_HOME'
pssh -i -h ~/parallel/hostfile 'echo $JAVA_HOME'
pssh -i -h ~/parallel/hostfile 'yarn classpath'
pssh -i -h ~/parallel/hostfile 'jps'
```

