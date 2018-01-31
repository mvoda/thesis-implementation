#!/usr/bin/env bash
ADDRESS=localhost

RAFT_PROCESS_JAR=raftProcessRewrite/target/raftProcessRewrite-0.1-jar-with-dependencies.jar
REACTIVE_JAR=reactiveRewrite/target/reactiveRewrite-0.1-jar-with-dependencies.jar

java -cp .:${RAFT_PROCESS_JAR}:${REACTIVE_JAR} rx.distributed.ServerApp ${ADDRESS} 5053
