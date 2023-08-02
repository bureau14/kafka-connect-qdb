#!/bin/bash
##
# This is a helper script for development, do not use for CI or anything
##
QDB_API_VERSION="3.14.0"

echo "Rebuilding JNI..."
rm -rf jni \
    && mkdir jni \
    && cd ../qdb-api-jni/ \
    && mvn compile \
    && rm -rf build \
    && mkdir build  \
    && cd build \
    && cmake -G Ninja .. \
    && cmake --build . \
    && cd .. \
    && mvn package -DskipTests \
    && cp target/jni* ../kafka-connect-qdb/jni/ \
    && cd ../kafka-connect-qdb/

echo "Installing JNI"
mvn install:install-file -f pom-jni.xml
mvn install:install-file -f pom-jni-arch.xml -Darch=linux-x86_64

mvn test
