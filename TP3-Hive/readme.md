* TP3 - Hive functions for Wordcount

###Introduction

First, I created a database firstdb :

<code><pre>CREATE DATABASE firstdb LOCATION `user/ltreguer/firstdb`</pre></code>

Then I copied the .csv file with the first name into my depository

<code><pre> 
hdfs dfs -mkdir prenoms
hdfs dfs -cp /res/prenoms.csv prenoms/0000 
</pre></code>

Then we create two tables : 

```
CREATE TABLE prenomstestb (
    prenoms STRING, 
    gender ARRAY<STRING>,
    origin array<STRING>, 
    version DOUBLE)
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
###Questions (the same as in TP2)

- Q1 : Count first names by origin

<code><pre>SELECT count(*), org FROM prenomstestb_opt LATERAL VIEW explode(origin) adTable AS org GROUP BY org;</pre></code>

- Q2 : Count number of first names by number of origins

We use the following and we add concatenate to have a nicer output, for instance :
3origins 456

<code><pre>SELECT concat(size(origin),'origins'),count(origin) from prenomstestb_opt group by size(origin);</pre></code>

- Q3 : Count proportion in percentages of male and female percentage 

The following query works, but it launches 5 MapReduce Functions for some reason and takes about 40 seconds to complete.

```
select table1.countg/table2.counttotal, table1.gdr 
from ((select count(*) as countg,gdr from prenomstestb_opt lateral view explode(gender) adtableb as gdr group by gdr) table1,
      (select count(*) as counttotal from prenomstestb_opt) table2);
```

This query is perfectible. 
Getting the number of first names and the number of male and female occurences with the following queries only take 0.06sec and 

```
select count(*) from prenomstestb_opt;

sh
```



