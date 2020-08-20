# manage_flights.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
#
#
# B. Bird - 06/28/2020

import sys, csv, psycopg2, datetime

if len(sys.argv) < 2:
    print("Usage: %s <input file>"%sys.argv[0],file=sys.stderr)
    sys.exit(1)
    
input_filename = sys.argv[1]

conn = psycopg2.connect(dbname='your_name',
                        user='your_name',
                        password='your_pwd',
                        host='your_db',
                        port=your_port)
cursor = conn.cursor()

with open(input_filename) as f:
    for row in csv.reader(f):
        if len(row) == 0:
            continue #Ignore blank rows
        action = row[0]
        if action.upper() == 'DELETE':
            if len(row) != 2:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                #Maybe abort the active transaction and roll back at this point?
                break
            flight_id = row[1]
            
            #Handle the DELETE action here
            delete_statement = cursor.mogrify("""DELETE FROM flights WHERE flight_id = %s;""" % flight_id)
            print(delete_statement)

            try:
                cursor.execute(delete_statement)
            except:
                e = sys.exc_info()[0]
                print(e,file=sys.stderr)

                conn.rollback()
                cursor.close()
                conn.close()

                sys.exit(1)
            if cursor.rowcount == 0:
                print("failed to delete row or no such row exist", file=sys.stderr)

        elif action.upper() in ('CREATE','UPDATE'):
            if len(row) != 8:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                #Maybe abort the active transaction and roll back at this point?
                break

            flight_id = row[1]
            airline = row[2]
            src,dest = row[3],row[4]
            departure, arrival = row[5],row[6]
            aircraft_id = row[7]

            #Handle the "CREATE" and "UPDATE" actions here
            if (action.upper() == 'CREATE'):
                create_statement = cursor.mogrify(
                    """insert into flights values('%s', '%s', '%s', '%s', '%s', '%s', '%s');"""%
                    (flight_id, src, dest, aircraft_id, airline, departure, arrival))
                print(create_statement)

                try:
                    cursor.execute(create_statement)
                    
                except:
                    e = sys.exc_info()[0]
                    print(e,file=sys.stderr)

                    conn.rollback()
                    cursor.close()
                    conn.close()

                    sys.exit(1)

                if cursor.rowcount == 0:
                    print("failed to create flight", file = sys.stderr)

            else:
                update_statement = cursor.mogrify(
                    """UPDATE flights SET origin = %s, destination = %s, aircraft_id = %s, airline = %s, departure_time = %s, arrival_time = %s WHERE flight_id = %s;""",
                    (src, dest, aircraft_id, airline, departure, arrival, flight_id)
                )
                print(update_statement)

                try:
                    cursor.execute(update_statement)

                except:
                    e = sys.exc_info()[0]
                    print(e,file=sys.stderr)

                    conn.rollback()
                    cursor.close()
                    conn.close()

                    sys.exit(1)

                if cursor.rowcount == 0:
                    print("failed to create flight", file = sys.stderr)

        else:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            #Maybe abort the active transaction and roll back at this point?
            break
        
conn.commit()
cursor.close()
conn.close()
