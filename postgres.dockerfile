FROM postgres:12.4-alpine

COPY scripts/postgres_init.sql /docker-entrypoint-initdb.d/
