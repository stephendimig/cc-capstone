/* 
File:   group2_1.pig
Author: Stephen Dimig
Description: This file is an Apache Pig script that loads all of the cleaned data set and returns the
top 10 carriers from each airport.
*/

-- Load data
Flights = LOAD '/user/root/airline_ontime/*.txt' USING PigStorage(',') as (rec:int,date,flightno:int,origin,dest,unique_carrier, carrier, arrival_time, arrival_delay:int, arrival_delay_min:int, dep_time, dep_delay:int, dep_delay_min:int, dow:int);

-- Group by origin and carrier
DepFlights = FOREACH Flights GENERATE origin, unique_carrier, dep_delay_min;  
ByOriginAndCarrier = GROUP DepFlights BY (origin, unique_carrier); 

-- Find average departure delay, order by carrier, sort, and limit to 10.
A  = FOREACH ByOriginAndCarrier GENERATE FLATTEN(group) AS (origin, unique_carrier), AVG(DepFlights.dep_delay_min) as dep_delay_avg;
B = GROUP A BY origin;
C = FOREACH B { sorted = ORDER A BY dep_delay_avg; lim = LIMIT sorted 10; GENERATE FLATTEN(lim); };

STORE C INTO '/user/root/output/pig';  
