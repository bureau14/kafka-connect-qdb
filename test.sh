#!/bin/bash
##
# This is a helper script for development, do not use for CI or anything
##
QDB_API_VERSION="3.1.0-SNAPSHOT"

echo "Cleaning java"
rm -rf java \
   && mkdir java

echo "Rebuilding JNI..."
cd ../qdb-api-java \
    && rm -rf jni \
    && mkdir jni \
    && cd ../qdb-api-jni/ \
    && rm -rf ./build \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make -j32 \
    && cp -v ./jni* ../../qdb-api-java/jni/ \
    && cd ../../qdb-api-java

echo "Installing JNI"
mvn install:install-file -f pom-jni.xml
mvn install:install-file -f pom-jni-arch.xml -Darch=linux-x86_64

echo "Building Java"
mvn package -DskipTests \
    && cp -v target/qdb-*.jar ../kafka-connect-qdb/java \
    && cd ../kafka-connect-qdb

echo "Installing Java"
mvn install:install-file -f pom-java.xml -DpomFile=pom-java.xml

mvn test
