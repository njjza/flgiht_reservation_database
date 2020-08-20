DROP TABLE IF EXISTS 
        Airports,
        Aircraft,
        Flights,
        passengers,
        reservations,
    CASCADE
;

DROP FUNCTION IF EXISTS
    reservation_one_book_limit,
    flight_airline_consistency,
    fligt_intl_aiport_consistency,
    fligt_time_location_consistency,
    flight_reservation_limit,
    passenger_unique_warning_silencer
;

DROP TRIGGER IF EXISTS reservation_one_book_limit ON Reservations;
DROP TRIGGER IF EXISTS flight_airline_consistency ON Flights;
DROP TRIGGER IF EXISTS fligt_intl_aiport_consistency ON Flights;
DROP TRIGGER IF EXISTS fligt_time_location_consistency ON Flights;
DROP TRIGGER IF EXISTS flight_reservation_limit ON Flights;
DROP TRIGGER IF EXISTS passenger_unique_warning_silencer ON Passengers;
;

CREATE TABLE Airports (
    id                  char(3), 
    name                varchar(255) Not NULL,
    country             varchar(255) Not NULL,
    international       boolean,

    check ( upper (id) = id ),
    check ( length (name) < 255 ),
    check ( length (country) < 255 ),
    
    primary key (id)
);

CREATE Table Aircraft (
    id                  varchar(64),
    airline             varchar(255),
    model               varchar(255),
    passenger_capacity  int

    check ( passenger_capacity >= 0 ),
    check ( Not (airline IS NULL OR model IS NULL OR id IS NULL)),

    primary key (id)
);

CREATE Table Flights (
    flight_id          int,
    origin              char(3),
    destination         char(3),
    aircraft_id       varchar(64),     
    airline             varchar(255),
    departure_time      timestamp,
    arrival_time        timestamp
    
    check (arrival_time > departure_time)
    check (origin <> destination),
    check (length(airline) < 255),
    -- check international flight --
    
    PRIMARY KEY (flight_id),

    FOREIGN KEY (origin) REFERENCES Airports (id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
    ,
    FOREIGN KEY (destination) REFERENCES Airports (id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
    ,
    FOREIGN KEY (aircraft_id) REFERENCES Aircraft (id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

CREATE TABLE Passengers (
    person_id   int NOT NULL,
    name        varchar(1000) NOT NULL

    check (length (name) < 1001),
    primary key (person_id)
);

CREATE TABLE Reservations (
    person_id       int,
    flight_id      int,

    foreign key (person_id) REFERENCES Passengers (person_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
    ,
    foreign key (flight_id) REFERENCES Flights (flight_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

-- Airline Consistency --
CREATE OR REPLACE FUNCTION flight_airline_consistency() RETURNS TRIGGER AS 
    $BODY$
        DECLARE
            varchar_tmp varchar(255);

        BEGIN
            SELECT airline INTO varchar_tmp FROM aircraft WHERE aircraft.airline = NEW.airline 
                AND NEW.aircraft_id = aircraft.id;

            IF NOT FOUND THEN RAISE EXCEPTION 'flight airline and aircraft airline does not match';
            END IF;

            RETURN NULL;
        END;
    $BODY$
LANGUAGE plpgsql;

-- International Airport Consistency --
CREATE OR REPLACE FUNCTION fligt_intl_aiport_consistency() RETURNS TRIGGER AS 
    $BODY$
        DECLARE
            airports_tmp1    airports%ROWTYPE;
            airports_tmp2    airports%ROWTYPE;
            int_tmp         int;

        BEGIN     
            SELECT * INTO airports_tmp1 FROM airports
                WHERE airports.id = NEW.destination;
            SELECT * INTO airports_tmp2 FROM airports
                WHERE airports.id = NEW.origin;

            IF  (
                    airports_tmp1.country <> airports_tmp2.country and 
                    (airports_tmp1.international = FALSE OR 
                    airports_tmp2.international = FALSE)
                )
            THEN
                RAISE NOTICE 'triggered'; 
                RAISE EXCEPTION 'Both airports need to be international airport for an international travel';
                RETURN NULL;
            END IF;

            RETURN NEW;

        END;
    $BODY$
LANGUAGE plpgsql;

-- flight time & location consistency --
-- POST INSERT/UPDATE BEFORE COMMIT
CREATE OR REPLACE FUNCTION fligt_time_location_consistency() RETURNS TRIGGER AS 
    $BODY$
        DECLARE
            tmp                 record;
            flights_tmp         Flights%ROWTYPE;
            flights_prev        Flights%ROWTYPE;
            flights_next        Flights%ROWTYPE;
            rank_tmp            int;

        BEGIN
            FOR tmp IN
                WITH
                    cte AS (
                        SELECT  flight_id as _flight_id,
                                lead(flight_id) over(partition by aircraft_id order by departure_time) as next
                        FROM flights

                        ORDER BY departure_time
                    )

                SELECT * FROM cte

            LOOP
                
                SELECT * INTO flights_tmp FROM flights WHERE flight_id = tmp._flight_id;
                SELECT * INTO flights_next FROM flights WHERE flight_id = tmp.next;

                IF flights_next.origin <> flights_tmp.destination THEN
                    RAISE EXCEPTION 'either origin or destination of the flight does not match with the previous or next flight';
                END IF;

                IF (extract(EPOCH FROM flights_next.departure_time) - 
                    extract(EPOCH FROM flights_tmp.arrival_time))/60 < 60 THEN
                    RAISE NOTICE 'here';
                    RAISE NOTICE 'flight1: %, flight2: %', flights_tmp.flight_id, flights_next.flight_id;
                    RAISE NOTICE 'interval: %', ((extract(EPOCH FROM flights_tmp.arrival_time) - 
                    extract(EPOCH FROM flights_next.departure_time))/60);
                    RAISE EXCEPTION 'make sure enough time interval between each flight is preserved';
                    END IF;

            END LOOP;
            RETURN NULL;
        END;
    $BODY$
LANGUAGE plpgsql;

-- Makesure not overbooking --
CREATE OR REPLACE FUNCTION flight_reservation_limit() RETURNS TRIGGER AS
    $BODY$
        DECLARE
            cur_reserved        int;
            max_reservation     int;

        BEGIN
            SELECT count(person_id) INTO cur_reserved FROM Reservations
                WHERE NEW.flight_id = reservations.flight_id;
            SELECT passenger_capacity iNTO max_reservation FROM (
                SELECT flight_id, passenger_capacity FROM flights NATURAL JOIN aircraft
            ) as T1
                WHERE NEW.flight_id = T1.flight_id;
            
            RAISE NOTICE 'cur_reserved: %', cur_reserved;
            RAISE NOTICE 'capacity: %', max_reservation;
            IF cur_reserved + 1 > max_reservation THEN
                RAISE EXCEPTION 'This flight is full';
            END IF;

            RETURN NEW;
        END;
    $BODY$
LANGUAGE plpgsql;

-- One researvation per flight per person --
CREATE OR REPLACE FUNCTION reservation_one_book_limit() RETURNS TRIGGER AS
    $BODY$
        DECLARE
            people      RECORD;
        BEGIN
            FOR people IN
                SELECT person_id FROM Reservations
                    WHERE NEW.flight_id = Reservations.flight_id
            LOOP
                IF NEW.person_id = people.person_id
                    THEN RAISE EXCEPTION 'This person have booked this flight';
                END IF;
            END LOOP;

            RETURN NEW;
        END;
    $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION passenger_unique_warning_silencer() RETURNS TRIGGER AS
    $BODY$
        DECLARE
            _id    int;
        BEGIN
            SELECT person_id INTO _id FROM passengers WHERE NEW.person_id = passengers.person_id;
            IF FOUND THEN RETURN NULL;
            END IF;
            RETURN NEW;
        END;    
    $BODY$
LANGUAGE plpgsql;

CREATE TRIGGER fligt_intl_aiport_consistency BEFORE INSERT OR UPDATE ON flights
    for each row execute procedure fligt_intl_aiport_consistency();

CREATE TRIGGER passenger_unique_warning_silencer BEFORE INSERT ON Passengers
    for each row execute procedure passenger_unique_warning_silencer();

CREATE TRIGGER flight_reservation_limit BEFORE INSERT OR UPDATE ON Reservations
    for each row execute procedure flight_reservation_limit();

CREATE TRIGGER reservation_one_book_limit BEFORE INSERT OR UPDATE ON Reservations
    for each row execute procedure reservation_one_book_limit();


CREATE TRIGGER flight_airline_consistency AFTER INSERT OR UPDATE ON flights
    for each row execute procedure flight_airline_consistency();

CREATE CONSTRAINT TRIGGER fligt_time_location_consistency AFTER INSERT OR UPDATE ON flights
    INITIALLY DEFERRED 
    for each row execute procedure fligt_time_location_consistency();


