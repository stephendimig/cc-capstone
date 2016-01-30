Flights = LOAD '/user/root/airline_ontime/*2008*.txt' USING PigStorage(',') as (rec:int,date:chararray,flightno:int,origin:chararray,dest:chararray,unique_carrier, carrier, arrival_time, arrival_delay:int, arrival_delay_min:int, dep_time:int, dep_delay:int, dep_delay_min:int, dow:int);

B = FILTER Flights by (origin == '$X' AND dest == '$Y' AND dep_time < 1200) OR (origin == '$Y' AND dest == '$Z' AND dep_time > 1200);
C = FOREACH B GENERATE flightno, origin, dest, unique_carrier, date, dep_time, arrival_delay_min;  

D = FILTER C BY arrival_delay_min is not null;

STORE D INTO '/user/root/output/pig';  

