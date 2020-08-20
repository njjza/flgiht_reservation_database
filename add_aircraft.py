# add_aircraft.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
#
#
# B. Bird - 06/28/2020

import sys
import csv
import psycopg2

if len(sys.argv) < 2:
    print("Usage: %s <input file>" % sys.argv[0], file=sys.stderr)
    sys.exit(1)

input_filename = sys.argv[1]

# Open your DB connection here
conn = psycopg2.connect(dbname='your_name',
                        user='your_name',
                        password='your_pwd',
                        host='your_db',
                        port=your_port)
cursor = conn.cursor()

with open(input_filename) as f:
    for row in csv.reader(f):
        if len(row) == 0:
            continue  # Ignore blank rows

        if len(row) != 4:
            print("Error: Invalid input line \"%s\"" %
                  (','.join(row)), file=sys.stderr)
            # Maybe abort the active transaction and roll back at this point?
            break

        aircraft_id, airline, model, seating_capacity = row

        insert_statement = cursor.mogrify(
            """insert into aircraft values( %s, %s, %s, %s );""", (aircraft_id, airline, model, seating_capacity))
        print(insert_statement)
        
        try:
            cursor.execute(insert_statement)

        except:
            e = sys.exc_info()[0]
            print(e,file=sys.stderr)

            conn.rollback()
            cursor.close()
            conn.close()

            sys.exit(1)
        
conn.commit()
cursor.close()
conn.close()
