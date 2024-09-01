-- Load the data from HDFS
data = LOAD '/piginput/s.txt' USING PigStorage(',') AS (id:int,name:charArray);
-- Dump the data to check if it was loaded correctly
DUMP data;

