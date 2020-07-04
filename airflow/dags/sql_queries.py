CREATE_TABLES = """
DROP TABLE IF EXISTS staging_yellow_trips;

CREATE TABLE IF NOT EXISTS staging_yellow_trips (
VendorID INT,
tpep_pickup_datetime TIMESTAMP,
tpep_dropoff_datetime TIMESTAMP,
passenger_count INT,
trip_distance FLOAT,
RatecodeID INT,
store_and_fwd_flag TEXT,
PULocationID INT,
DOLocationID INT,
payment_type FLOAT,
fare_amount FLOAT,
extra FLOAT,
mta_tax FLOAT,
tip_amount FLOAT,
tolls_amount FLOAT,
improvement_surcharge FLOAT,
total_amount FLOAT,
congestion_surcharge FLOAT
);

DROP TABLE IF EXISTS trips_lookup;

CREATE TABLE IF NOT EXISTS trips_lookup (
LocationID INT PRIMARY KEY,
Borough TEXT,
Zone TEXT,
service_zone TEXT
);

DROP TABLE IF EXISTS trips;

CREATE TABLE IF NOT EXISTS trips (
ID INT IDENTITY(0,1) PRIMARY KEY,
Date TEXT,
PULocationID INT,
DOLocationID INT,
passenger_count INT
);

DROP TABLE IF EXISTS pop_destination_passengers_month;

CREATE TABLE IF NOT EXISTS pop_destination_passengers_month(
month TEXT,
pick_up TEXT,
drop_off TEXT,
total_passengers INT,
ranking INT);

DROP TABLE IF EXISTS pop_destination_rides_month;

CREATE TABLE IF NOT EXISTS pop_destination_rides_month(
month TEXT,
pick_up TEXT,
drop_off TEXT,
total_rides TEXT,
ranking INT);


DROP TABLE IF EXISTS popular_rides_full;

CREATE TABLE IF NOT EXISTS popular_rides_full (
month TEXT,
pick_up TEXT,
drop_off TEXT,
ranking INT
);

DROP TABLE IF EXISTS cur_popular_dest;

CREATE TABLE IF NOT EXISTS cur_popular_dest (
pick_up TEXT,
drop_off TEXT,
ranking INT
);
"""

LOAD_TRIPS = """
--DELETE FROM trips;
INSERT INTO trips (Date, PULocationID, DOLocationID, passenger_count)
SELECT
	to_char(tpep_pickup_datetime, 'YYYY-MM'),
	PULocationID,
	DOLocationID,
	passenger_count
FROM staging_yellow_trips
WHERE passenger_count >= 0;
"""

COPY_LOOKUP_SQL = """
COPY trips_lookup
FROM 's3://nyc-tlc/misc/taxi _zone_lookup.csv'
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
IGNOREHEADER 1
DELIMITER ','
"""

COPY_TRIPS_SQL = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
IGNOREHEADER 1
DELIMITER ','
"""

CALC_pop_destination_passengers_month = """
--DELETE FROM pop_destination_passengers_month;
INSERT INTO pop_destination_passengers_month
WITH total_passengers AS (
	SELECT
		t.Date as month,
		p.zone as pick_up,
		d.zone as drop_off,
		sum(t.passenger_count) total_passengers
	FROM trips t
	LEFT JOIN trips_lookup p
	ON p.locationid = t.PULocationID
	LEFT JOIN trips_lookup d
	ON d.locationid = t.DOLocationID
	WHERE t.Date = '{}'
	GROUP BY t.Date, p.zone, d.zone

),
ranked_total_passengers AS (
	SELECT
		*,
		rank() OVER (PARTITION BY pick_up ORDER BY total_passengers DESC) as ranking
	FROM total_passengers
)
SELECT
	*
FROM ranked_total_passengers
WHERE ranking <= 5;
"""

CALC_pop_destination_rides_month = """
--DELETE FROM pop_destination_rides_month;
INSERT INTO pop_destination_rides_month
WITH total_rides AS (
	SELECT
		t.Date as month,
		p.Borough as pick_up,
		d.Borough as drop_off,
		count(t.ID) total_rides
	FROM trips t
	LEFT JOIN trips_lookup p
	ON p.locationid = t.PULocationID
	LEFT JOIN trips_lookup d
	ON d.locationid = t.DOLocationID
	WHERE t.Date = '{}'
	GROUP BY t.Date, p.Borough, d.Borough

),
ranked_borough_destination AS (
	SELECT
		*,
		rank() OVER (PARTITION BY pick_up ORDER BY total_rides DESC) as ranking
	FROM total_rides
)
SELECT
	*
FROM ranked_borough_destination;
"""

Load_popular_rides_full = """
INSERT INTO popular_rides_full
WITH total_rides AS (
	SELECT
		t.Date as month,
		p.Borough as pick_up,
		d.Borough as drop_off,
		count(t.ID) total_rides
	FROM trips t
	LEFT JOIN trips_lookup p
	ON p.locationid = t.PULocationID
	LEFT JOIN trips_lookup d
	ON d.locationid = t.DOLocationID
	WHERE t.Date = '{}'
	GROUP BY t.Date, p.Borough, d.Borough
),
ranked_borough_destination AS (
	SELECT
		month,
		pick_up,
		drop_off,
		rank() OVER (PARTITION BY pick_up ORDER BY total_rides DESC) as ranking
	FROM total_rides
),
prev_rank AS (
SELECT 
	*
FROM popular_rides_full
WHERE month = '{}'
)
SELECT
	current.*
FROM ranked_borough_destination current
LEFT JOIN prev_rank
ON prev_rank.pick_up = current.pick_up 
	AND prev_rank.drop_off = current.drop_off
	AND prev_rank.ranking = current.ranking
WHERE prev_rank.ranking IS NULL
AND current.ranking <= 10
;
"""
CALC_current_pop_dest = """
DELETE FROM cur_popular_dest;
INSERT INTO cur_popular_dest
WITH total_rides AS (
	SELECT
		t.Date as month,
		p.Borough as pick_up,
		d.Borough as drop_off,
		count(t.ID) total_rides
	FROM trips t
	LEFT JOIN trips_lookup p
	ON p.locationid = t.PULocationID
	LEFT JOIN trips_lookup d
	ON d.locationid = t.DOLocationID
	WHERE t.Date = '{}'
	GROUP BY t.Date, p.Borough, d.Borough
),
ranked_borough_destination AS (
	SELECT
		pick_up,
		drop_off,
		rank() OVER (PARTITION BY pick_up ORDER BY total_rides DESC) as ranking
	FROM total_rides
)
SELECT
	*
FROM ranked_borough_destination
WHERE ranking <= 10;
"""


