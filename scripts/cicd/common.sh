#!/usr/bin/env bash

set -eux -o pipefail

THIS_SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)
PROJECT_ROOT=$(cd -- "${THIS_SCRIPT_DIR}/../.." >/dev/null && pwd)
MVN_TARGET_DIR="${PROJECT_ROOT}/target"

JAVA_PATH=${JAVA_PATH:-}
MVN_PATH=${MVN_PATH:-}

if [[ -n "${JAVA_PATH}" ]]; then
    JAVA="${JAVA_PATH}"
else
    JAVA=java
fi

if [[ -n "${MVN_PATH}" ]]; then
    MVN="${MVN_PATH}"
else
    MVN=mvn
fi

JNI_VERSION=${JNI_VERSION:-3.15.0-SNAPSHOT}
QDB_JNI_ARCH_CLASSIFIER=${QDB_JNI_ARCH_CLASSIFIER:-}

if [[ -z "${QDB_JNI_ARCH_CLASSIFIER}" ]]; then
    case "$(uname)" in
        Linux)
            case "$(uname -m)" in
                arm64|aarch64)
                    QDB_JNI_ARCH_CLASSIFIER="linux-aarch64"
                    ;;
                x86_64)
                    QDB_JNI_ARCH_CLASSIFIER="linux-x86_64"
                    ;;
                *)
                    echo "Unable to infer JNI classifier for Linux architecture: $(uname -m)" >&2
                    exit 1
                    ;;
            esac
            ;;
        FreeBSD)
            QDB_JNI_ARCH_CLASSIFIER="freebsd-x86_64"
            ;;
        Darwin)
            case "$(uname -m)" in
                arm64|aarch64)
                    QDB_JNI_ARCH_CLASSIFIER="osx-aarch64"
                    ;;
                x86_64)
                    QDB_JNI_ARCH_CLASSIFIER="osx-x86_64"
                    ;;
                *)
                    echo "Unable to infer JNI classifier for macOS architecture: $(uname -m)" >&2
                    exit 1
                    ;;
            esac
            ;;
        MINGW*|MSYS*|CYGWIN*)
            QDB_JNI_ARCH_CLASSIFIER="windows-x86_64"
            ;;
        *)
            echo "Unable to infer JNI classifier for OS: $(uname)" >&2
            exit 1
            ;;
    esac
fi

export PROJECT_ROOT
export MVN_TARGET_DIR
export JAVA
export MVN
export JNI_VERSION
export QDB_JNI_ARCH_CLASSIFIER

echo "PROJECT_ROOT: ${PROJECT_ROOT}"
echo "QDB_JNI_ARCH_CLASSIFIER: ${QDB_JNI_ARCH_CLASSIFIER}"
echo "Detected java:"
"${JAVA}" -version
echo "Detected maven:"
"${MVN}" -version
