#!/bin/sh

CLASSDIR=./libs
JDBCDIR=$CLASSDIR/jdbc

cd Ssql
CLASSPATH=$CLASSPATH:$CLASSDIR/supersql.jar:$JDBCDIR/postgresql.jar:$JDBCDIR/sqlitejdbc-v056.jar:$CLASSDIR/json-simple-1.1.1.jar:$CLASSDIR/jsoup-1.7.2.jar:$CLASSDIR/servlet-api.jar:$CLASSDIR/log4j-1.2.17.jar:$CLASSDIR/commons-codec-1.10.jar java -Dfile.encoding=UTF-8 supersql.FrontEnd $*

