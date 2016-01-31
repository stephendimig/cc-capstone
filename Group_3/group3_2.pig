/* 
File:   group3_2.pig
Author: Stephen Dimig
Description: This file is an Apache Pig script that loads all of the cleaned data set and returns all of the flights
from 2008 that meet the criteria.
*/

-- Load data
Flights = LOAD '/user/root/airline_ontime/*2008*.txt' USING PigStorage(',') as (rec:int,date:chararray,flightno:int,origin:chararray,dest:chararray,unique_carrier, carrier, arrival_time, arrival_delay:int, arrival_delay_min:int, dep_time:int, dep_delay:int, dep_delay_min:int, dow:int);

-- Filter for data that meet the criteria
B = FILTER Flights by (origin == '$X' AND dest == '$Y' AND dep_time < 1200) OR (origin == '$Y' AND dest == '$Z' AND dep_time > 1200);

-- Generate data we need for further processing.
C = FOREACH B GENERATE flightno, origin, dest, unique_carrier, date, dep_time, arrival_delay_min;  

-- Filter out data with null average.
D = FILTER C BY arrival_delay_min is not null;

STORE D INTO '/user/root/output/pig';  

