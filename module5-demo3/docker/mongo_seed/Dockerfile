FROM mongo
COPY data.csv /
RUN chown mongodb:mongodb data.csv
CMD mongoimport --host mongo --db pluralsight --collection pluralsight --drop --type csv --file /data.csv --headerline
