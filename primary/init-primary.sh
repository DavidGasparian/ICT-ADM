#!/bin/bash
set -e # Если будет ошибка, то баш скрипт прекратится

# Создаем специального пользователя "replicator", который умеет только копировать данные
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'postgres';
EOSQL

# Разрешаем этому пользователю подключаться к базе по сети
echo "host replication replicator 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"