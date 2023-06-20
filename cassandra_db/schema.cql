-- init-script.cql

-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS streaming WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
CREATE TABLE IF NOT EXISTS streaming.raw_row (
value_row text PRIMARY KEY
);

-- Insert some data
INSERT INTO streaming.raw_row (value_row) VALUES ('AAAAA');
INSERT INTO streaming.raw_row (value_row) VALUES ('BBBBB');


SELECT * FROM streaming.raw_row;