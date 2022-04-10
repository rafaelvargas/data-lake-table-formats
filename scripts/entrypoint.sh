#!/bin/bash
export HADOOP_VERSION=3.2.0
export AWS_JAVA_SDK_VERSION=1.11.375
export HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar
export JAVA_HOME=/usr/local/openjdk-8

/opt/apache-hive-metastore-3.1.3-bin/bin/schematool -initSchema -dbType postgres
/opt/apache-hive-metastore-3.1.3-bin/bin/start-metastore
