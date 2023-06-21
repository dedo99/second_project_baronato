-- init-script.cql

-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS streaming WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
CREATE TABLE IF NOT EXISTS streaming.raw_row_dataset (key text PRIMARY KEY, value text);


SELECT * FROM streaming.raw_row;