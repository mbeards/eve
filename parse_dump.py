#!/usr/bin/env python
import sys
import csv
import sqlite3

conn = sqlite3.connect('market_db')

def setup_db():
  c = conn.cursor()
  
  queries = [
    "CREATE TABLE trades_input (orderid int, regionid int, systemid int, stationid int, typeid int, bid int, price real, minvolume int, volremain int, volenter int, issued text, duration text, range int, reportedby int, reportedtime text)",
    "CREATE TABLE station_prices (issued text, regionid int, systemid int, stationid int, typeid int, price real)",
    "CREATE TABLE system_prices (issued text, regionid int, systemid int, typeid int, price real)",
    "CREATE TABLE region_prices (issued text, regionid int, typeid int, price real)",
    "CREATE TABLE global_prices (issued text, typeid int, price real)",
    "CREATE TABLE types (typeid int, typename text, typeclass int, size real, published int, marketgroup int, groupid int, raceid int)"
  ]

  for query in queries:
    try:
      c.execute(query)
    except Exception as e:
      print e

def parse_dump(filename):
  print "Parsing data dump"
  c = conn.cursor()

  with open(filename, 'r') as f:
    csv_reader = csv.reader(f)

    f = False
    for row in csv_reader:
      if f:
        query = 'INSERT INTO trades_input VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, "{10}", "{11}", {12}, {13}, "{14}")'.format(*row)
        c.execute(query)
      else:
        f = True
  conn.commit()

def compute_average_prices():
  print "Computing average prices"
  c = conn.cursor()
  q = 'insert into station_prices select issued, regionid, systemid, stationid, typeid, avg(price) from trades_input group by regionid, systemid, stationid, typeid'
  c.execute(q)

  q = 'insert into system_prices select issued, regionid, systemid, typeid, avg(price) from station_prices'
  c.execute(q)

  q = 'insert into region_prices select issued, regionid, typeid, avg(price) from system_prices'
  c.execute(q)

  q = 'insert into global_prices select issued, typeid, avg(price) from region_prices'
  c.execute(q)

  q = 'select * from global_prices'
  c.execute(q)

  conn.commit()

def parse_typenames(filepath):
  print "Parsing types"
  '''
  Read type information from types.txt, adding it to the table types
  '''

  c = conn.cursor()

  with open(filepath, 'r') as infile:
    for line in infile:
      l = line.split('|')
      if len(l) != 8:
        continue
      elif 'typeid' in l[0]:
        #Got header.
        continue
      else:
        #Got a line of data
        type_id = int(l[0].strip())
        type_name = l[1].strip()
        type_class = int(l[2].strip()) if l[2].strip() != '' else 0  #may be null
        if l[3].strip() != '':
          size = float(l[3].strip())
        else:
          size = 0
        published = 1 if l[4].strip() == '1' else 0
        market_group = int(l[5].strip()) if l[5].strip() != '' else 0
        group_id = int(l[5].strip()) if l[6].strip() != '' else 0
        race_id = int(l[5].strip()) if l[7].strip() != '' else 0

        q = "insert into types values (%s, \"%s\", %s, %s, %s, %s, %s, %s)"%(type_id, type_name, type_class, size, published, market_group, group_id, race_id)
        c.execute(q)

  conn.commit()
      

if __name__ == "__main__":
  setup_db()
  parse_typenames("data/meta/types.txt")
  parse_dump(sys.argv[1])
  compute_average_prices()
