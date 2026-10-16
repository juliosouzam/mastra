#!/usr/bin/env bash
set -euo pipefail

# Optional advanced observability uses the current SCN to avoid missing late commits.
# This script only configures the disposable integration-test application user.
oracle_user_pattern='^[A-Za-z][A-Za-z0-9_$#]{0,127}$'
if [[ ! "${APP_USER:-}" =~ $oracle_user_pattern ]]; then
  echo 'APP_USER must be a simple Oracle username for the integration fixture.' >&2
  exit 1
fi
oracle_app_user=$(printf '%s' "$APP_USER" | tr '[:lower:]' '[:upper:]')

sqlplus -s / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = FREEPDB1;
GRANT EXECUTE ON SYS.DBMS_FLASHBACK TO "$oracle_app_user";
EXIT
SQL
