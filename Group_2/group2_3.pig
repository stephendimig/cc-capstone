/* 
File:   group2_3.pig
Author: Stephen Dimig
Description: This file is an Apache Pig script that loads all of the cleaned data set and returns the top-10 carriers 
in decreasing order of on-time arrival performance at Y from X..
*/

-- Load data
Flights = LOAD '/user/root/airline_ontime/*.txt' USING PigStorage(',') as (rec:int,date,flightno:int,origin,dest,unique_carrier, carrier, arrival_time, arrival_delay:int, arrival_delay_min:int, dep_time, dep_delay:int, dep_delay_min:int, dow:int);

-- Group by origin and carrier
DepFlights = FOREACH Flights GENERATE origin, dest, unique_carrier, arrival_delay_min;  
ByOriginAndCarrier = GROUP DepFlights BY (origin, dest, unique_carrier); 

-- Find average arrival delay, sort, and limit to 10.
A  = FOREACH ByOriginAndCarrier GENERATE FLATTEN(group) AS (origin, dest, unique_carrier), AVG(DepFlights.arrival_delay_min) as arrival_delay_avg;
B = GROUP A BY (origin, dest);
C = FOREACH B { sorted = ORDER A BY arrival_delay_avg; lim = LIMIT sorted 10; GENERATE FLATTEN(lim); };
D = FILTER C BY arrival_delay_avg is not null;

STORE D INTO '/user/root/output/pig';  

