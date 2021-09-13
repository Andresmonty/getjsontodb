import json
from json import load
# import the psycopg2 database adapter for PostgreSQL
from urllib.request import urlopen
from psycopg2 import connect, Error
# import psycopg2's 'json' using an alias
from psycopg2.extras import json as psycop_json
import sys
import requests
import time
import psycopg2
import datetime as dt

def getDateMessage():
  current = dt.datetime.now()
  date = current.strftime('%Y-%m-%d')
  hour = current.strftime('%H:%M:%S.%f')
  return date + ' ' + hour

with urlopen("https://jsonplaceholder.typicode.com/todos") as json_data : source = json_data.read()
data = json.loads(source)
values = [list(x.values()) for x in data] 
#print(values)
columns = [list(x.keys()) for x in data] [0] 
#print (columns)
values_str = ""
for i, record in enumerate(values) :
    val_list = []
    for v, val in enumerate(record) :
        if type (val) == str : 
            val = "'{}'".format(val.replace("'", "''"))
        val_list += [ str(val) ]
    values_str += "(" + ', '.join( val_list ) + ", " + 'current_timestamp' + "),\n" 
values_str = values_str[:-2] + ";"


table_name = "json"
sql_string = "INSERT INTO " + table_name + "(" + ','.join(columns) + ", created_at" + ")" " VALUES " + values_str 
#sql_string = "INSERT INTO %s (%s)\nVALUES\n%s" % (
#    table_name,
#    ', '.join(columns) + ", created_at" ,
#    values_str
#)
#print("\nSQL string:\n\n")
#mydate = getDateMessage()
print(sql_string)
try:
    conn = psycopg2.connect(user="json", password="json", database="jsondb", host="10.128.0.2", port='5432')
    print("Successfully connected!")
    cur = conn.cursor()
    print ("\ncreated cursor object:", cur)
except (Exception, Error) as err:
    print ("\npsycopg2 connect error:", err)
    conn = None
    cur = None
if cur != None:

    try:
        cur.execute(sql_string)
        conn.commit()

        print ('\nfinished INSERT INTO execution')

    except (Exception, Error) as error:
        print("\nexecute_sql() error:", error)
        conn.rollback()

    # close the cursor and connection
    cur.close()
    conn.close()
