#!/bin/bash
if [ -z "${IMPORT_DIR}" ]
then
  export IMPORT_DIR="/root/go/src/github.com/LF-Engineering/dev-analytics-import-finos-identities"
fi
cd "${IMPORT_DIR}" || exit 1
MISSING_ORGS_CSV=finos_missing_orgs MISSING_PROFILES_CSV=finos_missing_profiles ORGS_MAP_FILE=./map_org_names.yaml REPLACE=1 COMPARE=1 PROJECT_SLUG=finos-f SH_DSN="`cat ./DB_CONN.prod.secret`" ./import-identities.sh prod
