#!/usr/bin/env bash

set -eux -o pipefail

THIS_SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)
source "${THIS_SCRIPT_DIR}/common.sh"

pushd "${PROJECT_ROOT}"

JNI_DIR="${PROJECT_ROOT}/jni"

find_single_jni_jar() {
    local description="$1"
    local exact_jar="$2"
    local pattern="$3"

    if [[ -f "${exact_jar}" ]]; then
        printf '%s\n' "${exact_jar}"
        return 0
    fi

    local matches=()
    while IFS= read -r -d '' jar; do
        matches+=("${jar}")
    done < <(find "${JNI_DIR}" -maxdepth 1 -type f -name "${pattern}" -print0 | sort -z)

    case "${#matches[@]}" in
        0)
            echo "Missing JNI ${description} jar: ${exact_jar}" >&2
            echo "Expected ${pattern} in ${JNI_DIR}" >&2
            exit 1
            ;;
        1)
            printf '%s\n' "${matches[0]}"
            ;;
        *)
            echo "Found multiple JNI ${description} jars matching ${pattern}:" >&2
            printf '  %s\n' "${matches[@]}" >&2
            echo "Remove the extra jars or provide ${exact_jar}." >&2
            exit 1
            ;;
    esac
}

find_base_jni_jar() {
    local exact_jar="${JNI_DIR}/jni-${JNI_VERSION}.jar"

    if [[ -f "${exact_jar}" ]]; then
        printf '%s\n' "${exact_jar}"
        return 0
    fi

    local matches=()
    local jar
    while IFS= read -r -d '' jar; do
        local filename
        filename=$(basename "${jar}")

        if [[ ! "${filename}" =~ ^jni-[0-9]+(\.[0-9]+)*(-SNAPSHOT)?\.jar$ ]]; then
            continue
        fi

        local is_classifier_jar=0
        for arch in "${JNI_CLASSIFIERS[@]}" "${QDB_JNI_ARCH_CLASSIFIER}"; do
            if [[ "${jar}" == *"-${arch}.jar" ]]; then
                is_classifier_jar=1
                break
            fi
        done

        if [[ "${is_classifier_jar}" -eq 0 ]]; then
            matches+=("${jar}")
        fi
    done < <(find "${JNI_DIR}" -maxdepth 1 -type f -name "jni-*.jar" -print0 | sort -z)

    case "${#matches[@]}" in
        0)
            echo "Missing JNI base jar: ${exact_jar}" >&2
            echo "Expected one base JNI jar (jni-<version>.jar or jni-<version>-SNAPSHOT.jar) in ${JNI_DIR}" >&2
            exit 1
            ;;
        1)
            printf '%s\n' "${matches[0]}"
            ;;
        *)
            echo "Found multiple JNI base jars:" >&2
            printf '  %s\n' "${matches[@]}" >&2
            echo "Remove the extra jars or provide ${exact_jar}." >&2
            exit 1
            ;;
    esac
}

BASE_JAR=$(find_base_jni_jar)

CLASSIFIER_JAR=$(find_single_jni_jar "${QDB_JNI_ARCH_CLASSIFIER}" "${JNI_DIR}/jni-${JNI_VERSION}-${QDB_JNI_ARCH_CLASSIFIER}.jar" "jni-*-${QDB_JNI_ARCH_CLASSIFIER}.jar")

for arch in "${JNI_CLASSIFIERS[@]}"; do
    find_single_jni_jar "${arch}" "${JNI_DIR}/jni-${JNI_VERSION}-${arch}.jar" "jni-*-${arch}.jar" >/dev/null
done

"${MVN}" install:install-file -f pom-jni.xml -Dfile="${BASE_JAR}"
for arch in "${JNI_CLASSIFIERS[@]}"; do
    classifier_jar=$(find_single_jni_jar "${arch}" "${JNI_DIR}/jni-${JNI_VERSION}-${arch}.jar" "jni-*-${arch}.jar")
    "${MVN}" install:install-file -f pom-jni-arch.xml -Darch="${arch}" -Dfile="${classifier_jar}"
done

popd
