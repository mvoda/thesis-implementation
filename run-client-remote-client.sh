#!/usr/bin/env bash
ADDRESS=localhost

RAFT_PROCESS_JAR=raftProcessRewrite/target/raftProcessRewrite-0.1-jar-with-dependencies.jar
REACTIVE_JAR=reactiveRewrite/target/reactiveRewrite-0.1-jar-with-dependencies.jar

java -DdistributedRx.jar=${REACTIVE_JAR} -cp .:${RAFT_PROCESS_JAR}:${REACTIVE_JAR} rx.distributed.ClientRemoteClientApp ${ADDRESS} 5000 ${ADDRESS} 5051 ${ADDRESS} 5052 ${ADDRESS} 5053
