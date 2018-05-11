#!/bin/sh

CLASSDIR='./libs'

CLASSPATH=$CLASSPATH:$CLASSDIR/gsp.jar:$CLASSDIR/jackson-annotations-2.8.0.jar:$CLASSDIR/jackson-core-2.8.1.jar:$CLASSDIR/jackson-databind-2.8.5.jar:$CLASSDIR/JSQLParser.jar:$CLASSDIR/mysql-connector-java-5.1.40-bin.jar:$CLASSDIR/postgresql-9.4.1211.jre6.jar:$CLASSDIR/sqlite-jdbc-3.16.1.jar:$CLASSDIR/commons-lang3-3.5.jar:$CLASSDIR/icu4j-59.1.jar:$CLASSDIR/RTA.jar java -Dfile.encoding=UTF-8 rtaclient.RTAClient

