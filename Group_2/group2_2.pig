/* 
File:   group2_2.pig
Author: Stephen Dimig
Description: This file is an Apache Pig script that loads all of the cleaned data set and returns the
top destination airports by departure time from each airport.
*/

-- Load data
Flights = LOAD '/user/root/airline_ontime/*.txt' USING PigStorage(',') as (rec:int,date,flightno:int,origin,dest,unique_carrier, carrier, arrival_time, arrival_delay:int, arrival_delay_min:int, dep_time, dep_delay:int, dep_delay_min:int, dow:int);

-- Group by origin and dest
DepFlights = FOREACH Flights GENERATE origin, dest, dep_delay_min;  
ByOriginAndDest = GROUP DepFlights BY (origin, dest); 

-- Find average departure delay, sort, and limit to 10.
A  = FOREACH ByOriginAndDest GENERATE FLATTEN(group) AS (origin, dest), AVG(DepFlights.dep_delay_min) as dep_delay_avg;
B = GROUP A BY origin;
C = FOREACH B { sorted = ORDER A BY dep_delay_avg; lim = LIMIT sorted 10; GENERATE FLATTEN(lim); };
D = FILTER C BY dep_delay_avg is not null;

STORE D INTO '/user/root/output/pig';  

