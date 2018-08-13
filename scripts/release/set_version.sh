#!/bin/sh
set -eu -o pipefail
IFS=$'\n\t'

if [[ $# -ne 1 ]] ; then
    >&2 echo "Usage: $0 <new_version>"
    exit 1
fi

INPUT_VERSION=$1; shift

MAJOR_VERSION=${INPUT_VERSION%%.*}
WITHOUT_MAJOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.}
MINOR_VERSION=${WITHOUT_MAJOR_VERSION%%.*}
WITHOUT_MINOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.${MINOR_VERSION}.}
PATCH_VERSION=${WITHOUT_MINOR_VERSION%%.*}

XYZ_VERSION="${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"

if [[ "${INPUT_VERSION}" == *-* ]] ; then
    TAGS_VERSION="-SNAPSHOT"
else
    TAGS_VERSION=
fi

FULL_XYZ_VERSION="${XYZ_VERSION}${TAGS_VERSION}"

cd $(dirname -- $0)
cd ${PWD}/../..

sed -i -e 's/\(QDB_API_VERSION="\)[^"]*\("\)/\1'"${FULL_XYZ_VERSION}"'\2/' test.sh

sed -i -e '/<groupId>net.quasardb<\/groupId>/,/<\/\(version\)>/ s/<version>[0-9.]*[0-9]\(-SNAPSHOT\)\?<\/version>/<version>'"${FULL_XYZ_VERSION}"'<\/version>/' pom.xml pom-java.xml pom-jni.xml pom-jni-arch.xml
sed -i -e '/<groupId>net.quasardb<\/groupId>/,/<\/properties>/ s/<file>\([-a-zA-Z_/]*\)[0-9.]*[0-9]\(-SNAPSHOT\)\?\([-.${}a-zA-Z]*\)<\/file>/<file>\1'"${FULL_XYZ_VERSION}"'\3<\/file>/' pom.xml pom-java.xml pom-jni.xml pom-jni-arch.xml
