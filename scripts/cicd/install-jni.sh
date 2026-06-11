#!/usr/bin/env bash

set -eux -o pipefail

THIS_SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)
source "${THIS_SCRIPT_DIR}/common.sh"

pushd "${PROJECT_ROOT}"

JNI_DIR="${PROJECT_ROOT}/jni"
BASE_JAR="${JNI_DIR}/jni-${JNI_VERSION}.jar"

if [[ ! -f "${BASE_JAR}" ]]; then
    echo "Missing JNI base jar: ${BASE_JAR}" >&2
    echo "Expected jni-*.jar in ${JNI_DIR}" >&2
    exit 1
fi

CLASSIFIER_JAR="${JNI_DIR}/jni-${JNI_VERSION}-${QDB_JNI_ARCH_CLASSIFIER}.jar"
if [[ ! -f "${CLASSIFIER_JAR}" ]]; then
    echo "Missing JNI classifier jar: ${CLASSIFIER_JAR}" >&2
    echo "Expected ${QDB_JNI_ARCH_CLASSIFIER} JNI jar in ${JNI_DIR}" >&2
    exit 1
fi

"${MVN}" install:install-file -f pom-jni.xml
"${MVN}" install:install-file -f pom-jni-arch.xml -Darch="${QDB_JNI_ARCH_CLASSIFIER}"

popd
