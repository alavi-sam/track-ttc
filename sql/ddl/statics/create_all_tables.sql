CREATE TABLE IF NOT EXISTS agency (
    agency_id INTEGER PRIMARY KEY,
    agency_name TEXT,
    agency_url TEXT,
    agency_timezone TEXT,
    agency_lang TEXT,
    agency_phone TEXT
);

CREATE TABLE IF NOT EXISTS calendar_dates (
    service_id INTEGER,
    date TEXT,
    exception_type INTEGER,
    PRIMARY KEY (service_id, date)
);

CREATE TABLE IF NOT EXISTS calendar (
    service_id INTEGER PRIMARY KEY,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
);

CREATE TABLE IF NOT EXISTS routes (
    route_id INTEGER PRIMARY KEY,
    agency_id INTEGER FOREIGN KEY REFERENCES agency(agency_id),
    route_short_name TEXT,
    route_long_name TEXT,
    route_desc TEXT,
    route_type INTEGER,
    route_url TEXT,
    route_color TEXT,
    route_text_color TEXT
);

CREATE TABLE IF NOT EXISTS shapes (
    shape_id INTEGER PRIMARY KEY,
    shape_pt_lat REAL,
    shape_pt_lon REAL,
    shape_pt_sequence INTEGER,
    shape_dist_traveled REAL
);

CREATE TABLE IF NOT EXISTS trips (
    route_id INTEGER FOREIGN KEY REFERENCES routes(route_id),
    service_id INTEGER FOREIGN KEY REFERENCES calendar(service_id),
    trip_id INTEGER PRIMARY KEY,
    trip_headsign TEXT,
    direction_id INTEGER,
    block_id INTEGER,
    shape_id INTEGER FOREIGN KEY REFERENCES shapes(shape_id),
    trip_type INTEGER
);

CREATE TABLE IF NOT EXISTS stops (
    stop_id INTEGER PRIMARY KEY,
    stop_code TEXT,
    stop_name TEXT,
    stop_desc TEXT,
    stop_lat REAL,
    stop_lon REAL,
    zone_id INTEGER,
    stop_url TEXT,
    location_type INTEGER,
    parent_station INTEGER,
    stop_timezone TEXT,
    wheelchair_boarding INTEGER
);

CREATE TABLE IF NOT EXISTS stop_times (
    trip_id INTEGER FOREIGN KEY REFERENCES trips(trip_id),
    arrival_time TEXT,
    departure_time TEXT,
    stop_id INTEGER FOREIGN KEY REFERENCES stops(stop_id),
    stop_sequence INTEGER,
    stop_headsign TEXT,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    shape_dist_traveled REAL
);