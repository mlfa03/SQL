/* MODELING TIME SERIES DATA FROM INFORMATION ON TEMPERATURE MEASUREMENTS */

/* PART 1: Loading the data */

COPY time_series.location_temp(event_time, location_id, temp_celsius)
FROM 'data/location_temp.txt' DELIMITER ',';

/* Visualizing the data */
SELECT * FROM time_series.location_temp
ORDER BY event_time, location_id  /* Ordering the view by event time and location */
LIMIT 100 

/* Getting the average temperatures by Location */
SELECT location_id, avg(temp_celsius)
FROM time_series.location_temp
GROUP BY location_id;

/* Creating an Index to try to improve the performance */
CREATE INDEX idx_loc_temp_location_id ON time_series.location_temp(location_id);


/* execution location with index */
SELECT location_id, avg(temp_celsius)
FROM time_series.location_temp
GROUP BY location_id;


/* Executing again, this time limiting the location to loc2 */
SELECT location_id, avg(temp_celsius)
FROM time_series.location_temp
WHERE location_id = 'loc2'
GROUP BY location_id;
/* Now the cost of executing the query is lower because we just executed a subset of the data */

/* PART 2: Time and Location */

CREATE INDEX idx_loc_temp_time_loc ON time_series.location_temp(event_time,location_id);


SELECT location_id, avg(temp_celsius)
FROM time_series.location_temp
WHERE event_time BETWEEN '2019-03-05' AND '2019-03-06'
GROUP BY location_id;


DROP INDEX time_series.idx_loc_temp_location_id;


/* PART 3: Partitioning the table by time */

/* Create a table of location and temperature measurements */

CREATE TABLE time_series.location_temp_p
(
    event_time timestamp NOT NULL,
    event_hour integer,
    temp_celsius integer,
    location_id character varying COLLATE pg_catalog."default"
)
 PARTITION BY RANGE (event_hour);

/* Starting table partition: */

CREATE TABLE time_series.loc_temp_p1 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (0) TO (2);
CREATE INDEX idx_loc_temp_p1 ON time_series.loc_temp_p1(event_time);


CREATE TABLE time_series.loc_temp_p2 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (2) TO (4);
CREATE INDEX idx_loc_temp_p2 ON time_series.loc_temp_p2(event_time);


CREATE TABLE time_series.loc_temp_p3 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (4) TO (6);
CREATE INDEX idx_loc_temp_p3 ON time_series.loc_temp_p3(event_time);


CREATE TABLE time_series.loc_temp_p4 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (6) TO (8);
CREATE INDEX idx_loc_temp_p4 ON time_series.loc_temp_p4(event_time);


CREATE TABLE time_series.loc_temp_p5 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (8) TO (10);
CREATE INDEX idx_loc_temp_p5 ON time_series.loc_temp_p5(event_time);


CREATE TABLE time_series.loc_temp_p6 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (10) TO (12);
CREATE INDEX idx_loc_temp_p6 ON time_series.loc_temp_p6(event_time);


CREATE TABLE time_series.loc_temp_p7 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (12) TO (14);
CREATE INDEX idx_loc_temp_p7 ON time_series.loc_temp_p7(event_time);


CREATE TABLE time_series.loc_temp_p8 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (14) TO (16);
CREATE INDEX idx_loc_temp_p8 ON time_series.loc_temp_p8(event_time);


CREATE TABLE time_series.loc_temp_p9 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (16) TO (18);
CREATE INDEX idx_loc_temp_9 ON time_series.loc_temp_p9(event_time);


CREATE TABLE time_series.loc_temp_p10 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (18) TO (20);
CREATE INDEX idx_loc_temp_p10 ON time_series.loc_temp_p10(event_time);


CREATE TABLE time_series.loc_temp_p11 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (20) TO (22);
CREATE INDEX idx_loc_temp_p11 ON time_series.loc_temp_p11(event_time);


CREATE TABLE time_series.loc_temp_p12 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (22) TO (24);
CREATE INDEX idx_loc_temp_p12 ON time_series.loc_temp_p12(event_time);




INSERT INTO time_series.location_temp_p
                       ( event_time, event_hour, temp_celsius, location_id)
                       (SELECT event_time, extract(hour from event_time), temp_celcius, location_id
                        FROM time_series.location_temp);

/* Querying partitioned table */

SELECT location_id, avg(temp_celcius)
FROM time_series.location_temp
WHERE event_time BETWEEN '2019-03-05' AND '2019-03-06'
GROUP BY location_id;


SELECT location_id, avg(temp_celcius)
FROM time_series.location_temp_p
WHERE event_time BETWEEN '2019-03-05' AND '2019-03-06'
GROUP BY location_id;


SELECT location_id, avg(temp_celcius)
FROM time_series.location_temp_p
WHERE event_hour BETWEEN 0 and 4
GROUP BY location_id;

