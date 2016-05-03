from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

from collections import Counter
import uuid
import pandas as pd
import math
import datetime as dt

BATCH_SIZE=10000
# Nodes will be running on: 192.168.56.11, 192.168.56.12, 192.168.56.13
HOSTS = ['192.168.56.11', '192.168.56.12', '192.168.56.13']

class FlightModel(Model):
    __table_name__ = "flights"
    __keyspace__ = "flughafen"
    id = columns.Integer(primary_key=True)
    year = columns.Integer()
    day_of_month = columns.Integer()
    fl_date = columns.DateTime()
    airline_id = columns.Integer()
    carrier = columns.Text()
    fl_num = columns.Integer()
    origin_airport_id = columns.Integer()
    origin = columns.Text()
    origin_city_name = columns.Text()
    origin_state_abr = columns.Text()
    dest = columns.Text()
    dest_city_name = columns.Text()
    dest_state_abr = columns.Text()
    dep_time = columns.DateTime()
    arr_time = columns.DateTime()
    # The exercise description gives the following two
    # fields as timestamp, though given the use case
    # and the limitations on the timestamp data type
    # it would be better to keep these as integer types
    # reflecting time travelled in minutes (or milliseconds)
    actual_elapsed_time = columns.Integer()
    air_time = columns.Integer()
    distance = columns.Integer()

class Departures(Model):
    # minimal information to show departing flights
    # from a given airport
    __table_name__ = "departures"
    __keyspace__ = "flughafen"
    id = columns.Integer(primary_key=True)
    # clustering order specified for departure time
    dep_time = columns.DateTime(primary_key=True, clustering_order="ASC")
    origin= columns.Text(index=True)
    carrier = columns.Text()
    fl_num = columns.Integer()
    dest = columns.Text()

class Airtime(Model):
    # minimal information to show departing flights
    # from a given airport
    __table_name__ = "airtime"
    __keyspace__ = "flughafen"
    fl_num = columns.Integer(primary_key=True, partition_key=True)
    # clustering order specified for airtime
    airtime_bucket = columns.Integer(primary_key=True, clustering_order="ASC")
    id = columns.Integer(primary_key=True)
    carrier = columns.Text()
    origin= columns.Text()
    dest = columns.Text()

def initialize_keyspace():
    cluster = Cluster(HOSTS)
    session = cluster.connect()

    # ensure that the desired keyspace exists
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS flughafen
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    """)

    session.set_keyspace('flughafen')
    # Make sure that the table does not exist
    session.execute("DROP TABLE IF EXISTS flights;")
    cluster.shutdown()


# TODO: point the object mapper connection at the existing session
def load_csv():

    # create the table using the FlightModel
    sync_table(FlightModel)

    # load the data from a csv
    df = pd.read_csv("data/flights_from_pg.csv", header=None)

    df.columns = ['id', 'year', 'day_of_month', 'fl_date', 'airline_id', 'carrier',
                  'fl_num', 'origin_airport_id', 'origin', 'origin_city_name',
                  'origin_state_abr', 'dest', 'dest_city_name', 'dest_state_abr',
                  'dep_time', 'arr_time', 'actual_elapsed_time', 'air_time', 
                  'distance']

    # MUNGE: dep_time and arr_time have 2400 values - change those to 0000
    df.dep_time[df.dep_time == 2400] = 0
    df.arr_time[df.arr_time == 2400] = 0

    # add the date parts to the departure and arrival times
    padtime = lambda x: "%04d" % x
    df.dep_time = df.fl_date + " " + df.dep_time.apply(padtime)
    df.arr_time = df.fl_date + " " + df.arr_time.apply(padtime)
    print(df.ix[1])
    #(1048576, 1)
    # convert all the timestamp types to pandas datetimes
    df.fl_date = pd.to_datetime(df.fl_date)
    df.dep_time = pd.to_datetime(df.dep_time)
    df.arr_time = pd.to_datetime(df.arr_time)

    print(df.shape)
    df.apply( lambda r: FlightModel.create(**r.to_dict()), axis=1)
    # cqlsh> select count(*) from flughafen.flights;
    #  count
    # ---------
    #  1048576
    a_airports = len(list(filter( lambda x: x[0]=='A', list(df.origin.unique()) )))
    print("Number of airport codes starting with A: %d" % a_airports)
'''
with BatchQuery() as b:
    # add each of the records to cassandra in batch fashion
    # with each batch of size BATCH_SIZE
    while st != None and st != len(df) :
        en = st + BATCH_SIZE
        en = None  if en > len(df)  else en
        if en != None:
            print("batch offs: %d, %d" % (st,en))
        else:
            print("last segment")
'''

def load_departures():
    #print("FlightModel records loaded: %d" % FlightModel.objects().limit(None).count())
    ct = 0
    for fl in FlightModel.objects().all().limit(None):
        if ct < 10:
            print(fl)
            ct += 1
        Departures.create(id=fl['id'], origin=fl['origin'], dep_time=fl['dep_time'],
                          carrier=fl['carrier'], fl_num=fl['fl_num'], dest=fl['dest'])
    #print("records in departures: %d" % Departures.objects().all().count())

def load_airtime():
    print("loading airtime table")
    ct = 0
    for fl in FlightModel.objects().all().limit(None):
        if ct % 10000 == 0:
            print("processed airtime rows: %d" % ct)
        Airtime.create(fl_num=fl['fl_num'], id=fl['id'], carrier=fl['carrier'],
                       origin=fl['origin'], dest=fl['dest'],
                       airtime_bucket=10*math.floor(fl['air_time']/10))
    #print("records in airtime: %d" % Airtime.objects().all().count())
    for user in Airtime.objects().limit(5):
        print(user)

def run_queries():
    lbound = dt.datetime( 2012, 1, 25)
    ubound = dt.datetime( 2012, 1, 26)
    q = Departures.objects().filter(Departures.origin == 'HNL')
    q = q.filter(Departures.dep_time >= lbound)
    q = q.filter(Departures.dep_time < ubound)
    ct_hnl = len(q)
    print("HNL departures on 2012-01-25: %d" % ct_hnl)
    lbound = dt.datetime( 2012, 1, 23)
    ubound = dt.datetime( 2012, 1, 24)
    q = Departures.objects().filter(Departures.dep_time >= lbound)
    q = q.filter(Departures.dep_time < ubound)
    f23 = Counter( r.origin for r in q )
    f23 = f23.most_common(2)
    print("Airport with most flights %s - %d" % f23[0])

# syntax for reusing session is flaky, just shutdown the cluster and reconnect with the object mapper
connection.setup(HOSTS, "cqlengine", protocol_version=3)
# load_csv()
#sync_table(Departures)
# load_departures()
#sync_table(Airtime)
#load_airtime()
run_queries()
