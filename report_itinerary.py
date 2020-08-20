# report_itinerary.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
#
#
# B. Bird - 06/28/2020

import psycopg2, sys, datetime

conn = psycopg2.connect(dbname='your_name',
                        user='your_name',
                        password='your_pwd',
                        host='your_db',
                        port=your_port)
cursor = conn.cursor()

def print_header(passenger_id, passenger_name):
    print("Itinerary for %s (%s)"%(str(passenger_id), str(passenger_name)) )
    
def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model):
    print("Flight %-4s (%s):"%(flight_id, airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time, arrival_time,duration_minutes))
    print("    %s -> %s (%s: %s)"%(source_airport_name, dest_airport_name, aircraft_id, aircraft_model))


if len(sys.argv) < 2:
    print('Usage: %s <passenger id>'%sys.argv[0], file=sys.stderr)
    sys.exit(1)

passenger_id = sys.argv[1]

statement = cursor.mogrify("""SELECT name FROM passengers where person_id = %s;""", (passenger_id,))
cursor.execute(statement)
person_name = cursor.fetchone()[0]
print_header(passenger_id, person_name)

statement = cursor.mogrify("""SELECT flight_id FROM reservations where person_id = %s;""", (passenger_id,))
cursor.execute(statement)
flights = cursor.fetchall()

for flight_id in flights:
    statement = cursor.mogrify("""SELECT origin, airline, destination, departure_time, arrival_time, (extract(epoch from arrival_time) - extract(epoch from departure_time)), aircraft_id FROM flights where flight_id = %s;""", (flight_id,))
    cursor.execute(statement)
    source_airport_name, airline, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id = cursor.fetchone()
    duration_minutes = round(duration_minutes / 60)

    statement = cursor.mogrify("""SELECT model FROM aircraft where id = %s;""", (aircraft_id,))
    cursor.execute(statement)
    model = cursor.fetchone()[0]

    statement = cursor.mogrify("""SELECT name FROM airports where id = %s;""", (source_airport_name,))
    cursor.execute(statement)
    source_airport_name = cursor.fetchone()[0]

    statement = cursor.mogrify("""SELECT name FROM airports where id = %s;""", (dest_airport_name,))
    cursor.execute(statement)
    dest_airport_name = cursor.fetchone()[0]

    print_entry(flight_id[0], airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, model)
