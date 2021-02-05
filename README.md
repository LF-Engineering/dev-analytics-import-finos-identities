# import-identities

Import FINOS identities YAML file.

# Running locally

- Go to/clone `LF-Engineering/dev-analytics-import-sh-json` and start a local MariaDB dockerized instance: `PASS=rootpwd ./mariadb_local_docker.sh`.
- Dump the production database: `mysqldump -hHOST -uUSER -pPASSWORD DB > dump.sql`. You can get HOST/USER/PASSWORD/DB values from secret files.
- Initialize the empty SoringHat database: `USR=root PASS=rootpwd SH_USR=sortinghat SH_PASS=pwd SH_DB=sortinghat ./mariadb_init.sh`.
- Restore `prod` dump on this new database: `mysql -h127.0.0.1 -P13306 -prootpwd -uroot sortinghat < dump.sql`.
- Run the import locally (as cron executes it): `IMPORT_DIR="`realpath .`" ./finos_local.sh local`.
- If you just want to run import on already fetched file: `` ST='' DEBUG=1 DEBUG_SQL=1 MISSING_ORGS_CSV=finos_missing_orgs MISSING_PROFILES_CSV=finos_missing_profiles ORGS_MAP_FILE=../dev-analytics-affiliation/map_org_names.yaml REPLACE='' COMPARE=1 PROJECT_SLUG=finos-f SH_DSN="`cat ../da-ds-gha/DB_CONN.local.secret`" ./import-identities ./identities.yaml ``.


# Prod deployment

- Deploy cron job that will run `finos_prod.sh`: `crontab -e`, add entry from `cron/finos_prod.crontab`.
- Copy `finos_prod.sh` to `/usr/bin/`: `cp finos_prod.sh /usr/bin`.
