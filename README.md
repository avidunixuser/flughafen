# DataStax Enterprise Exercise


### Q1: Load up at least a one node cluster of DSE

Adapted a vagrant-ansible recipe for spinning up a three node cluster plus opscenter.

See vagrant-ansible-cassandra and setup_cluster.sh for the broad strokes
-- tweaks to config not shown

### Q2: Create the base data bodel using the given table defn

See implementation of FlightModel class in airtraffic.py

### Q3: Load source data from csv into flights table using driver based application

See load_csv function in airtraffic.py

### Q4: Create tables to answer these questions

a. Build a query table to list all flights leaving a particular airport sorted by time.

See implementation of Departures class in airtraffic.py

b. List the carrier, origin, and destination airport for a flight based on 10 min buckets of air_time

See definition of the Airtime class and the load_airtime function in airtraffic.py


### Q5: Answer these questions using DSE and the created tables

a. How many flights originated from the 'HNL' airport code on 2012-01-25?
```
    cqlsh> select count(*) from flughafen.departures where origin = 'HNL' and dep_time >= '2012-01-25' and dep_time < '2012-01-26' ;
    count
    -------
    288
```

b. How many airport codes start with the letter 'A'?

```
    >> #unique origin airports starting with A
    >> airports = list(filter( lambda x: x[0]=='A', list(df.origin.unique())))
    >> len(airports)
    22
```

c. What originating airport had the most flights on 2012-01-23?

```
    # top 5 airports by number of flights on 2012-01-23
    ~ » cqlsh --cqlversion=3.2.1 --request-timeout=600 192.168.56.11 9042 -e "select origin from flughafen.departures where dep_time >= '2012-01-23' and dep_time < '2012-01-24' allow filtering;" | sort | uniq -c | sed -e 's/^\s\+//' | sort -nr -k1 | head -n 5
    2155     ATL
    1860     ORD
    1827     DFW
    1342     DEN
    1287     LAX
    ```
