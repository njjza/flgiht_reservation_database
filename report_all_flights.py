# report_all_flights.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
#
#
# B. Bird - 06/29/2020

import psycopg2, sys, datetime

conn = psycopg2.connect(dbname='your_name',
                        user='your_name',
                        password='your_pwd',
                        host='your_db',
                        port=your_port)
cursor = conn.cursor()


def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model, seating_capacity, seats_full):
    print("Flight %s (%s):" % (flight_id, airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time,arrival_time,duration_minutes))
    print("    %s -> %s"%(source_airport_name,dest_airport_name))
    print("    %s (%s): %s/%s seats booked"%(aircraft_id, aircraft_model,seats_full,seating_capacity))


cursor.execute("""SELECT * FROM flights GROUP BY flight_id""")
flights = cursor.fetchall()

for flight in flights:
    flight_id = flight[0]
    source_airport = flight[1]
    dest_airport = flight[2]
    aircraft_id = flight[3]
    airline = flight[4]
    departure_time = flight[5]
    arrival_time = flight[6]
    time_delta = int((arrival_time - departure_time).total_seconds()/60)

    statement = cursor.mogrify("""SELECT name from airports where id = %s""", (source_airport,))
    cursor.execute(statement)
    source_airport = cursor.fetchone()[0]

    statement = cursor.mogrify("""SELECT name from airports where id = %s""", (dest_airport,))
    cursor.execute(statement)
    dest_airport = cursor.fetchone()[0]

    statement = cursor.mogrify("""SELECT model, passenger_capacity FROM aircraft WHERE id = %s;""", (aircraft_id,))
    cursor.execute(statement)
    model, seating_capacity = cursor.fetchone()

    statement = cursor.mogrify("""SELECT count(*) FROM reservations WHERE flight_id = %s;""", (flight_id,))
    cursor.execute(statement)
    reserverd = cursor.fetchone()[0]

    print_entry(flight_id, airline, source_airport, dest_airport, departure_time, arrival_time, time_delta, aircraft_id, model, seating_capacity,reserverd)
