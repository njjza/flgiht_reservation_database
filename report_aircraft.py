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

def print_entry(aircraft_id, airline, model_name, num_flights, flight_hours, avg_seats_full, seating_capacity):
    print("%-5s (%s): %s"%(aircraft_id, model_name, airline))
    print("    Number of flights : %d"%num_flights)
    print("    Total flight hours: %d"%flight_hours)
    print("    Average passengers: (%.2f/%d)"%(avg_seats_full,seating_capacity))
    
cursor.execute("""SELECT * FROM aircraft GROUP BY id""")
aircrafts = cursor.fetchall()

for aircraft in aircrafts:
    aircraft_id = aircraft[0]
    airline = aircraft[1]
    model = aircraft[2]
    seating_capacity = aircraft[3]

    statement = cursor.mogrify("""select sum(extract(epoch from arrival_time) - extract(epoch from departure_time)) 
    from flights where aircraft_id = %s;""", (aircraft_id,))
    cursor.execute(statement)
    flight_hours = cursor.fetchone()[0]
    flight_hours = round(flight_hours/(60*60)) if (flight_hours is not None) else 0 
    
    statement = cursor.mogrify("""SELECT count(flight_id) FROM flights WHERE aircraft_id = %s;""", (aircraft_id,))
    cursor.execute(statement)
    num_flights = cursor.fetchone()[0]

    if (num_flights == 0):
        avg_seats_full = 0
    else:
        statement = cursor.mogrify("""SELECT count(person_id) FROM reservations 
        NATURAL JOIN flights where aircraft_id = %s;""", (aircraft_id,))
        cursor.execute(statement)
        total_people = cursor.fetchone()[0]
        avg_seats_full = total_people/num_flights

    print_entry(aircraft_id, airline, model, num_flights, flight_hours, avg_seats_full, seating_capacity)
