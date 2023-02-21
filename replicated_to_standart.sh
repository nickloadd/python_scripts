#!/bin/bash

# bash script to convert from replicated table to standart table

OUTDIR=.

DB_USER=$2
DB_PASS=$4

if [[ ! $2 ]] || [[ ! $4 ]]
then
echo "Usage: $0 -u DB_USERNAME -p DB_PASSWORD"
exit 1
fi

while read -r db ; do
  while read -r table ; do

  if [ "$db" == "system" ] || [ "$db" == "information_schema" ] || [ "$db" == "INFORMATION_SCHEMA" ]; then
     echo "skip system db"
     continue 2;
  fi

  if [[ "$table" == ".inner."* ]] || [[ "$table" == ".*distributed"* ]]; then
     echo "skip materialized view $table ($db)"
     continue;
  fi

  echo "export table $table from database $db"

    # dump schema
    clickhouse-client -u ${DB_USER} --password ${DB_PASS} --host 127.0.0.1 -q "SHOW CREATE TABLE ${db}.${table}" > "${OUTDIR}/${db}_${table}.sql"
    # add ';' in end of tables sql script
    sed 's/$/\;/g' "${OUTDIR}/${db}_${table}.sql" > "${OUTDIR}/tmp" && mv "${OUTDIR}/tmp" "${OUTDIR}/${db}_${table}.sql"
    # '\n' -> new line
    awk '{gsub(/\\n/,"\n")}1' "${OUTDIR}/${db}_${table}.sql" > "${OUTDIR}/tmp" && mv "${OUTDIR}/tmp" "${OUTDIR}/${db}_${table}.sql"
    # drop excess '\'
    sed 's/\\//g' "${OUTDIR}/${db}_${table}.sql" > "${OUTDIR}/tmp" && mv "${OUTDIR}/tmp" "${OUTDIR}/${db}_${table}.sql"
    # 'replicated' engine -> standart engine 
    sed -E "s/ENGINE\ =\ Replicated/ENGINE\ =\ /g"  "${OUTDIR}/${db}_${table}.sql" > "${OUTDIR}/tmp" && mv "${OUTDIR}/tmp" "${OUTDIR}/${db}_${table}.sql"
    # drop replicated settings from engine config
    sed -E "s/MergeTree.*/MergeTree()/g" "${OUTDIR}/${db}_${table}.sql" >> "${OUTDIR}/non_replicated_schema.sql"

  done < <(clickhouse-client -u ${DB_USER} --password ${DB_PASS} --host 127.0.0.1 -q "SHOW TABLES FROM $db")
done < <(clickhouse-client -u ${DB_USER} --password ${DB_PASS} --host 127.0.0.1 -q "SHOW DATABASES")