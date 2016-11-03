USE ltreguer;

SELECT count(*), org FROM prenoms_opt LATERAL VIEW explode(origin) adTable AS org GROUP BY org;

SELECT concat(size(origin),'origins'),count(origin) from prenoms_opt group by size(origin);

select table1.countg/table2.counttotal, table1.gdr 
from ((select count(*) as countg,gdr from prenoms_opt lateral view explode(gender) adtableb as gdr group by gdr) table1,
      (select count(*) as counttotal from prenoms_opt) table2);
