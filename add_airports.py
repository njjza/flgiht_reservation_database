# add_airports.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
#
#
# B. Bird - 06/28/2020

import sys, csv, psycopg2

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

        if len(row) != 4:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            #Maybe abort the active transaction and roll back at this point?
            break

        airport_code,airport_name,country,international = row

        if international.lower() not in ('true','false'):
            print('Error: Fourth value in each line must be either "true" or "false"',file=sys.stderr)
            #Maybe abort the active transaction and roll back at this point?
            break
        international = international.lower() == 'true'

        insert_statement = cursor.mogrify(
            "insert into airports values( %s, %s, %s, %s);", (airport_code, airport_name, country, international))
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
		
		
