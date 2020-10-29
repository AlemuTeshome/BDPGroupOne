-- table block2 --
/* 
We download boundary and water bodies shape file of the Netherlands.
we cliped the public block using the netherlands boundary on QGIS. 
Then, we removed blocks laied on water bodies and load it into our schema as true_blocks.
We made a difference geoprocessing operation to remove the water bodies from the true_blocks.
we load the block data into our schema from the QGIS layer as block_w_w.
Identifying and separating the dublicate values in the block_w_w.
*/
create table s2278502.dup_block2 
as (select b1.id, b1.block,b1.longit, b1.latit, b1.geom, b1.urlnr 
  from s2278502.true_blocks b1, s2278502.true_blocks b2
  where st_equals(b1.geom, b2.geom) and b1.block <> b2.block)
-- table unique_block2 --								 
-- create a table called unique_block2 containing blocks which didn't belongs in duplicate blocks 
create table s2278502.unique_block2 
  as (select *from s2278502.true_blocks 
  where block not in(select distinct block 
  from s2278502.dup_block2))
-- adding the unique blocks from the duplicate blocks into the unique_block2.												   
insert into s2278502.unique_block2 
  select distinct on (longit,latit) id, geom, block, longit, latit, urlnr 
  from s2278502.dup_block2;
-- table block --
-- block table contains all the attributes needed by the model.
create table block as select * from block2;
-- create a table called observtion2017 which contains all observations recorded between Jan-July 2017.
create table s2278502.observation2017 as 
  select * from public.observation as o
  WHERE o.obsdate >= '2017-01-01'::date AND o.obsdate <='2017-06-30'::date;
--create a table called unique_block in our schema having unique block id by removing duplicate records.
create table s2278502.unique_block 
  as (select * from s2278502.true_blocks where block not in 
  (select distinct on (block, longit, latit) block from s2278502.dup_block)
)
-- table observers --
create table s2278502.road_access as select b.block, 
  sum(r.roadlength) as totallength 
  from public.block_road_access as r, s2278502.block2 as b
  where b.block=r.block group by(b.block,r.block) order by r.block;
-- add the total road length of each block in block table.
alter table s2278502.block add column totallength numeric;
UPDATE s2278502.block as b
  SET totallength = r.totallength
  FROM road_access as r
  WHERE b.block = r.block;
update s2278502.block set totallength=0 where totallength is NULL;

-- table demography --
-- replace the unknown value to zero
create table s2278502.demography as 
  select s.block, p.geom, p.aantal_inw as population, p.stedelijkh as city_index 
  from s2278502.block as s, public.demography as p 
  where st_contains(s.the_geom,p.geom) order by s.block;

update s2278502.demography set population=0 where population<0;
update s2278502.demography set city_index=0 where city_index<0;
/*
alter and update the block table so that 
it contains the total population and city like index of each block.
*/
alter table s2278502.block add column total_population numeric
  ,add column city_index numeric;
-- calculate and store the number of population per block.
create table pop_per_block as select d.block, sum(d.population) as population 
  from s2278502.demography as d 
  group by d.block order by d.block;
create table city_index as select d.block, sum(d.city_index) as city_index 
  from s2278502.demography as d 
  group by d.block order by d.block;
alter table pop_per_block drop column city_index;

UPDATE s2278502.block as b SET total_population = p.population 
  FROM s2278502.pop_per_block as p WHERE b.block = p.block;

UPDATE s2278502.block as b SET city_index = c.city_index 
  FROM s2278502.city_index as c WHERE b.block = c.block;

update s2278502.block set city_index=0 where city_index is null;
update s2278502.block set total_population=0 where total_population is null;

-- table weighted_landuse --
-- table landuse --
alter table s2278502.block add column landuse numeric;
create table weighted_landuse as select l.block, max(l.weight) as weight 
  from landuse as l, block as b
  where b.block = l.block group by l.block order by l.block; 

update landuse set weight = 1 where category = 'Bos' 
  or category = 'Nat natuurlijk terrein' or category = 'nature reserves';
update landuse set weight = 0 where category = 'Spoorweg' 
  or category = 'Hoofdweg';
update landuse set weight = 0 where weight <> 1;
-- table observation --
create table s2278502.observation as 
   select o.id, o.species, o.observer, o.obsdate, 
   o.block, o.longit, o.latit, o.obstime, o.idd
   from observation2017 as o, block as b 
   where b.block=o.block;
   
-- table total_obs --

/*
first calculate the total number of observers per block. then add the number of 
of observers of each block in the block table.
*/
create table s2278502.total_obs as select block, count(o) as total_observer
  from( select block,count(observer) as o 
	 from s2278502.observation group by(block, observer)) 
	 as citizen group by (block);
alter table s2278502.block add column total_observer int;
UPDATE s2278502.block as b
  SET total_observer = obs.total_observer
  FROM total_obs as obs
  WHERE b.block = obs.block;
update s2278502.block set total_observer=0 where total_observer is null;

-- table block_obs_date --
-- contains the number of observers per block per day.
create table block_obs_date as select tem.block, count(tem.tempobs) 
  as block_day_obs, tem.obsdate 
  from (select block, count(observer) as tempobs,obsdate 
    from observation group by (block,observer,obsdate))
    as tem group by (tem.block, tem.obsdate) order by(tem.block,tem.obsdate);

-- table temperature --
-- contains the temperature of each block per day.
create table temperature as select t.block, t.temper, t.dtime 
  from temperature as t inner join block as b on b.block=t.block
  where dtime between 20170101 and 20170630 order by (t.block,t.dtime);
-- table precipitation --
create table s2278502.precipitation as select p.block, p.precip, p.dtime 
  from public.precipitation as p inner join s2278502.block as b on b.block=p.block
  where dtime between 20170101 and 20170630 order by (p.block,p.dtime);
-- table true_data2 --
create table true_data2 as select t.block, t.dtime ::text, t.temper,p.precip
  from temperature as t inner join precipitation as p on t.dtime=p.dtime 
  where t.block=p.block;
-- change the date column to date format.
ALTER TABLE true_data2
  ALTER dtime DROP DEFAULT
 ,ALTER dtime type date USING dtime::date
 ,ALTER dtime SET DEFAULT '1970-01-01'::date;
-- adding the observers per block per day into true_data2.
alter table s2278502.true_data2 add column observer int;
UPDATE s2278502.true_data2 as t
  SET observer = bod.block_day_obs
  FROM block_obs_date as bod
  WHERE bod.block = t.block and bod.obsdate=t.dtime;
-- add the population, the city index and landuse weight in the true_data2.
alter table s2278502.true_data2 add column road_length numeric;
alter table s2278502.true_data2 add column city_index numeric;
alter table s2278502.true_data2 add column totalpopulation int;
alter table s2278502.true_data2 add column landuse numeric;
alter table s2278502.true_data add column total_observer int;
UPDATE s2278502.true_data2 as t
  SET road_length = b.totallength,
  city_index = b.city_index,
  totalpopulation = b.total_population,
  landuse = b.landuse
  FROM s2278502.block as b
  WHERE b.block = t.block;

UPDATE s2278502.true_data2 as t
  SET totalpopulation = b.total_population,
  city_index=b.city_index
  FROM s2278502.block as b
  WHERE b.block = t.block;

UPDATE s2278502.true_data2 as t
  SET landuse = b.landuse
  FROM s2278502.block as b
  WHERE b.block = t.block;
update s2278502.true_data2 as t set observer =0 where t.observer is null;

-- extracting the data which have an observer intensity record.
create table s2278502.observer_only as select * 
  from s2278502.true_data2 
  where observer > 0;
 
alter table s2278502.true_data2 add column biodiversity numeric;
update s2278502.true_data2 as t set biodiversity = b.aves2012_2016
  from public.biodiversity as b
  where t.block=b.block;

alter table s2278502.true_data add column biodiversity numeric;
update s2278502.true_data as t set biodiversity = b.aves2012_2016
  from public.biodiversity as b
  where t.block=b.block;

UPDATE true_data2
  SET observer = biodiversity, biodiversity = observer;
ALTER TABLE true_data2 RENAME COLUMN biodiversity TO observation;
ALTER TABLE true_data2 RENAME COLUMN observer TO biodiversity;

alter table true_data2 add column week int;
update true_data2 set week=(select dow from public.days where odate=dtime);
alter table true_data2 add column num_week int;
/****************************************************************************
update true_data2 set num_week=101 
  where dtime <= '2017-01-08' and (week=0 or week=6);
update true_data2 set num_week=102 
  where dtime > '2017-01-08' and dtime <= '2017-01-15' and (week=0 or week=6);
update true_data2 set num_week=103 
  where dtime > '2017-01-15' and dtime <= '2017-01-22' and (week=0 or week=6);
update true_data2 set num_week=104 
  where dtime > '2017-01-22' and dtime <= '2017-01-29' and (week=0 or week=6);

update true_data2 set num_week=105 
  where dtime > '2017-01-29' and dtime <= '2017-02-05' and (week=0 or week=6);
update true_data2 set num_week=106 
  where dtime > '2017-02-05' and dtime <= '2017-02-12' and (week=0 or week=6);
update true_data2 set num_week=107 
  where dtime > '2017-02-12' and dtime <= '2017-02-19' and (week=0 or week=6);
update true_data2 set num_week=108 
  where dtime > '2017-02-19' and dtime <= '2017-02-26' and (week=0 or week=6);

update true_data2 set num_week=109 
  where dtime > '2017-02-26' and dtime <= '2017-03-05' and (week=0 or week=6);
update true_data2 set num_week=110 
  where dtime > '2017-03-05' and dtime <= '2017-03-12' and (week=0 or week=6);
update true_data2 set num_week=111 
  where dtime > '2017-03-12' and dtime <= '2017-03-19' and (week=0 or week=6);
update true_data2 set num_week=112 
  where dtime > '2017-03-19' and dtime <= '2017-03-26' and (week=0 or week=6);

update true_data2 set num_week=113 
  where dtime > '2017-03-26' and dtime <= '2017-04-02' and (week=0 or week=6);
update true_data2 set num_week=114 
  where dtime > '2017-04-02' and dtime <= '2017-04-09' and (week=0 or week=6);
update true_data2 set num_week=115 
  where dtime > '2017-04-09' and dtime <= '2017-04-16' and (week=0 or week=6);
update true_data2 set num_week=116 
  where dtime > '2017-04-16' and dtime <= '2017-04-23' and (week=0 or week=6);

update true_data2 set num_week=117 
  where dtime > '2017-04-23' and dtime <= '2017-04-30' and (week=0 or week=6);
update true_data2 set num_week=118 
  where dtime > '2017-04-30' and dtime <= '2017-05-07' and (week=0 or week=6);
update true_data2 set num_week=119 
  where dtime > '2017-05-07' and dtime <= '2017-05-14' and (week=0 or week=6);
update true_data2 set num_week=120 
  where dtime > '2017-05-14' and dtime <= '2017-05-21' and (week=0 or week=6);

update true_data2 set num_week=121 
  where dtime > '2017-05-21' and dtime <= '2017-05-28' and (week=0 or week=6);
update true_data2 set num_week=122 
  where dtime > '2017-05-28' and dtime <= '2017-06-05' and (week=0 or week=6);
update true_data2 set num_week=123 
  where dtime > '2017-06-05' and dtime <= '2017-06-12' and (week=0 or week=6);
update true_data2 set num_week=124 
  where dtime > '2017-06-12' and dtime <= '2017-06-19' and (week=0 or week=6);
update true_data2 set num_week=125 
  where dtime > '2017-06-19' and dtime <= '2017-06-26' and (week=0 or week=6);

update true_data2 set num_week=126 
  where dtime > '2017-06-26' and dtime <= '2017-07-02' and (week=0 or week=6);
***********************************************************/
/*
It is aggregating the data in a weekly manner. 
We defined the observer intensity as the number of observers per block per week.
*/
create table true_data3
  as select block,num_week, avg(temper) as temperature, avg(precip) 
  as precipitation, sum(observation) as observation
  from true_data2 group by (block,num_week);
  
alter table true_data3 add column totalpopulation int;
alter table true_data3 add column landuse numeric;
alter table true_data3 add column biodiversity numeric;
alter table true_data3 add column city_index numeric;
alter table true_data3 add column road_length numeric;
update true_data3 as t3 set landuse =t2.landuse, city_index =t2.city_index,
  totalpopulation=t2.totalpopulation, road_length=t2.road_length,
  biodiversity=t2.biodiversity
  from true_data2 as t2
  where t2.block=t3.block;
update true_data3 set totalpopulation=observation, observation=totalpopulation;
alter table true_data3 rename column totalpopulation  to observations;
alter table true_data3 rename column observation to totalpopulation;
create table week_data as select * from true_data3 where num_week<100;
create table weekend_data as select * from true_data3 where num_week>100;
/*
alter table block_list add column week26 numeric;
select * from block_list limit 100;
update block_list b set week26=s.num_obs from boss s where b.block=s.block;
create or replace view boss as select b.block, num_obs 
from block_list b, test3 t3 
where b.block=t3.block and weeks=26;
*/
create table test2w_pop as
select block,weeks,temperature,precip,road_length,biodiversity,city_index,observations
from test2;

