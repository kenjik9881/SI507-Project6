# Import statements
import psycopg2
import psycopg2.extras
from config import *
import csv


# Write code / functions to set up database connection and cursor here.
db_connection, db_cursor = None, None

def get_connection_and_cursor():
    global db_connection, db_cursor
    if not db_connection:
        try:
            if db_password != "":
                db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
                print("Success connecting to database")
            else:
                db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
        except:
            print("Unable to connect to the database. Check server and credentials.")
            sys.exit(1) # Stop running program if there's no db connection.

    if not db_cursor:
        db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

db_connection, db_cursor = get_connection_and_cursor()


# Write code / functions to create tables with the columns you want and all database setup here.
def setup_database():
    # Involves DDL commands
    # DDL --> Data Definition Language
    # CREATE, DROP, ALTER, RENAME, TRUNCATE

    # conn, cur = get_connection_and_cursor()
    db_cursor.execute("DROP TABLE IF EXISTS Sites")
    db_cursor.execute("DROP TABLE IF EXISTS States")

    #Create States table
    db_cursor.execute("CREATE TABLE IF NOT EXISTS States(ID SERIAL PRIMARY KEY, NAME VARCHAR(40) UNIQUE)")


    #Create Sites table
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Sites(ID SERIAL PRIMARY KEY, NAME VARCHAR(128) UNIQUE, TYPE VARCHAR(128), State_ID INTEGER REFERENCES States(ID), Location VARCHAR(255), Description TEXT)")


    # Save all changes
    db_connection.commit()
    print('Setup database complete')

setup_database()


# Write code / functions to deal with CSV files and insert data into the database here.
def insert_data(file_name, state_name):
    conn, cur = db_connection, db_cursor

    from csv import DictReader
    the_reader = DictReader(open(file_name, 'r')) #converts to dicitonary

    cur.execute("INSERT INTO States(Name) VALUES (%s) RETURNING ID", (state_name,))
    result = cur.fetchone()
    print(result)

    for line_dict in the_reader:
        cur.execute("""INSERT INTO Sites(Name, Type, State_Id, Location, Description)
            VALUES (%s, %s, %s, %s, %s)""",
            (line_dict['NAME'], line_dict['TYPE'], result['id'], line_dict['LOCATION'], line_dict['DESCRIPTION']))



# Make sure to commit your database changes with .commit() on the database connection.
db_connection.commit()


# Write code to be invoked here (e.g. invoking any functions you wrote above)
insert_data('arkansas.csv', 'Arkansas')
insert_data('california.csv', 'California')
insert_data('michigan.csv', 'Michgan')


# Write code to make queries and save data in variables here.
def execute_and_print(query):
    conn, cur = db_connection, db_cursor
    cur.execute(query)
    results = cur.fetchall()
    for r in results:
        print(r)
    print('--> Result Rows:', len(results))
    print()
    return results


print('Query the database for all of the **locations** of the sites')
# all_locations = execute_and_print('SELECT location from sites')
db_cursor.execute('SELECT location from sites')
all_locations = db_cursor.fetchall()
print(all_locations)

print("Query the database for all of the **names** of the sites whose **descriptions** include the word `beautiful`")
db_cursor.execute("""SELECT name FROM sites WHERE description ILIKE '%beautiful%'""")
beautiful_sites = db_cursor.fetchall()
print(beautiful_sites)

print("Query the database for the total number of **sites whose type is `National Lakeshore`")
db_cursor.execute("""SELECT Count (*) FROM sites WHERE type = 'National Lakeshore' """)
natl_lakeshores = db_cursor.fetchall()
print(natl_lakeshores)

print("Query your database for the **names of all the national sites in Michigan**")
db_cursor.execute("""SELECT "sites"."name" FROM "sites" INNER JOIN "states" ON ("sites"."state_id") = ("states"."id") WHERE ("sites"."state_id") = 3 """)
michigan_names = db_cursor.fetchall()
# for name in michigan_names:
#     print(name['name'])
print(michigan_names)

print("Query your database for the **total number of sites in Arkansas**")
db_cursor.execute("""SELECT Count (*) FROM "sites" INNER JOIN "states" ON ("sites"."state_id") = ("states"."id") WHERE ("sites"."state_id") = 1 """)
total_number_arkansas = db_cursor.fetchall()
print(total_number_arkansas)


# We have not provided any tests, but you could write your own in this file or another file, if you want.
