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
function remove_clones {
  rm -rf dev-analytics-finos-metadata dev-analytics-affiliation 1>/dev/null 2>/dev/null
}
function cleanup {
  remove_clones
  rm -rf "${lock_file}"
}
> "${lock_file}"
trap cleanup EXIT
remove_clones
git clone "${repo}" || exit 7
git clone https://github.com/LF-Engineering/dev-analytics-affiliation || exit 8
cp dev-analytics-finos-metadata/ssf-bitergia-affiliations.yaml ./identities.yaml || exit 9
cp dev-analytics-affiliation/map_org_names.yaml ./map_org_names.yaml || exit 10
remove_clones
echo 'would go!'
#./import-identities ./identities.yaml
