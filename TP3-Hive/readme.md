* TP3 - Hive functions for Wordcount

###Introduction

First, we create a database firstdb :

<code><pre>CREATE DATABASE firstdb LOCATION `user/ltreguer/firstdb`</pre></code>

Then we create two tables : 

```
CREATE TABLE prenomstestb (prenoms STRING, gender ARRAY<STRING>,origin array<STRING>, version DOUBLE)
ROW FORMAT 
DELIMITED FIELDS TERMINATED BY `\073` 
collection items terminated by ',' 
STORED AS TEXTFILE LOCATION `/user/ltreguer/prenoms`<code>

CREATE TABLE prenomstestb_opt(
    prenoms STRING,
    gender array<string>,
    origin array<string>,
    version DOUBLE)
    ROW FORMAT DELIMITED
    STORED AS ORC;
```
We copy the CSV into the table. 
```
LOAD DATA INPATH '/user/ltreguer/prenoms' INTO TABLE prenomstestb;

INSERT INTO TABLE prenomstestb_opt SELECT * FROM prenomstestb;
```
