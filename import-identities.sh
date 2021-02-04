#!/bin/bash
date
if [ -z "${1}" ]
then
  echo "$0: you need to specify environment as a 1st arg: test|prod"
  exit 1
fi
lock_file="/tmp/import-identities-${1}.lock"
if [ -f "${lock_file}" ]
then
  echo "$0: another import-identities \"$1\" instance is still running, exiting"
  exit 2
fi
if [ -z "${IMPORT_DIR}" ]
then
  export IMPORT_DIR="/root/go/src/github.com/LF-Engineering/dev-analytics-import-finos-identities"
fi
cd "${IMPORT_DIR}" || exit 3
git pull || exit 4
make || exit 5
repo="`cat repo_access.secret`"
if [ -z "$repo" ]
then
  echo "$0: missing repo_access.secret file"
  exit 6
fi
function cleanup {
  rm -rf "${lock_file}"
}
> "${lock_file}"
trap cleanup EXIT
rm -rf dev-analytics-finos-metadata 1>/dev/null 2>/dev/null
git clone "${repo}" || exit 7
cp dev-analytics-finos-metadata/ssf-bitergia-affiliations.yaml ./identities.yaml || exit 8
rm -rf dev-analytics-finos-metadata 1>/dev/null 2>/dev/null
./import-identities ./identities.yaml
