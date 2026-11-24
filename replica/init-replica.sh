#!/bin/bash
set -e

# Ждём primary
until pg_isready -h postgres-primary -p 5432 -U postgres; do
  echo "Waiting for primary..."
  sleep 2
done

# Очищаем папку
rm -rf /var/lib/postgresql/data/*
mkdir -p /var/lib/postgresql/data

# Копируем данные
export PGPASSWORD=postgres
pg_basebackup \
  -h postgres-primary \
  -p 5432 \
  -U replicator \
  -D /var/lib/postgresql/data \
  -Fp -Xs -P -R

chown -R postgres:postgres /var/lib/postgresql/data
chmod 700 /var/lib/postgresql/data

# Запускаем базу от имени postgres
exec gosu postgres postgres -c hot_standby=on