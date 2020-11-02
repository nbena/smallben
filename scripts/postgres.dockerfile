FROM postgres:12.4-alpine

COPY postgres_init.sql /docker-entrypoint-initdb.d/
