#!/bin/bash

# bash script to convert from standart table to replicated table

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

  if [[ "$table" == ".inner."* ]]; then
     echo "skip materialized view $table ($db)"
     continue;
  fi

  echo "export table $table from database $db"

    # dump schema
    clickhouse-client -u ${DB_USER} --password ${DB_PASS} --host 127.0.0.1 -q "SHOW CREATE TABLE ${db}.${table}" > "${OUTDIR}/${db}_${table}.sql"
    # ';' in end of tables sql script
    sed 's/$/\;/g' "${OUTDIR}/${db}_${table}.sql" > "${OUTDIR}/tmp" && mv "${OUTDIR}/tmp" "${OUTDIR}/${db}_${table}.sql"
    # '\n' -> new line
    awk '{gsub(/\\n/,"\n")}1' "${OUTDIR}/${db}_${table}.sql" > "${OUTDIR}/tmp" && mv "${OUTDIR}/tmp" "${OUTDIR}/${db}_${table}.sql"
    # drop excess '\'
    sed 's/\\//g' "${OUTDIR}/${db}_${table}.sql" > "${OUTDIR}/tmp" && mv "${OUTDIR}/tmp" "${OUTDIR}/${db}_${table}.sql"
    # standart engine -> 'replicated' engine 
    sed -E "s/ENGINE\ =\ /ENGINE\ =\ Replicated/g" "${OUTDIR}/${db}_${table}.sql" > "${OUTDIR}/tmp" && mv "${OUTDIR}/tmp" "${OUTDIR}/${db}_${table}.sql"
    # add replicated settings to engine config
    sed -E "s/MergeTree/MergeTree(\'\/clickhouse\/tables\/\{shard\}\/${db}\/${table}\'\, \'\{replica\}\')/g" "${OUTDIR}/${db}_${table}.sql" >> "${OUTDIR}/replicated_schema.sql"

  done < <(clickhouse-client -u ${DB_USER} --password ${DB_PASS} --host 127.0.0.1 -q "SHOW TABLES FROM $db")
done < <(clickhouse-client -u ${DB_USER} --password ${DB_PASS} --host 127.0.0.1 -q "SHOW DATABASES")
