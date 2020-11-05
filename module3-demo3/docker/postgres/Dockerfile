FROM library/postgres:12
COPY data.csv /
RUN chown postgres:postgres data.csv
COPY init.sql /docker-entrypoint-initdb.d/
