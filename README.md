<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

GeoYCSB (Yahoo! Cloud Serving Benchmark for Geo workloads)
====================================
The original benchmark YCSB measures databases performance based
on generated input. However, for geospatial workloads there is a need to 
have the same input to measure geospatial operations on the same data. 
Hence, the benchmark should be adapted to consume data from the file input 
and include spatial operations in workload measurements.

Credits
-----
* YCSB 
* Couchbase fork of YCSB
* Blogpost about creating input from JSON file in YCSB
* GeoYCSB paper with spatial benchmark for MongoDB and Couchbase
* MongoDB fork of YCSB with updates for Mongo 6
* This repository: adjustment (modernizing) and rewriting parts of GeoYCSB. ArangoDB support


Links
-----
* To get here, use https://ycsb.site
* [Our project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [The original announcement from Yahoo!](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)
* [Couchbase fork of YCSB](https://github.com/couchbaselabs/YCSB)
* [Reading JSON in YCSB - Couchbase blogpost](https://www.couchbase.com/blog/ycsb-json-implementation-for-couchbase-and-mongodb/)
* [MongoDB fork of YCSB](https://github.com/mongodb-labs/YCSB)
* [Scientific paper of GeoYCSB](https://doi.org/10.1016/j.bdr.2023.100368)
* [Spatial Benchmark](https://github.com/yuvrajkanwar/Spatial-Benchmark)

Getting Started
---------------

1. Download the [latest release of YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest):

    ```sh
    curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
    tar xfvz ycsb-0.17.0.tar.gz
    cd ycsb-0.17.0
    ```
    
2. Set up a database to benchmark. There is a README file under each binding 
   directory.

3. Run YCSB command. 

    On Linux:
    ```sh
    bin/ycsb.sh load basic -P workloads/workloada
    bin/ycsb.sh run basic -P workloads/workloada
    ```

    On Windows:
    ```bat
    bin/ycsb.bat load basic -P workloads\workloada
    bin/ycsb.bat run basic -P workloads\workloada
    ```

  Running the `ycsb` command without any argument will print the usage. 
   
  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.

Geo extension
-------------
GeoYCSB contains geospatial workloads to benchmark databases performance on the real input. 
For this, the `GeoWorkload.java` is created to be able to use the real GeoJSON data 
and generate queries with geospatial operations. 
In addition, GeoYCSB introduces new workload settings:

* `geo_insert`
* `geo_update`
* `geo_scan`
* `geo_near`
* `geo_box`
* `geo_intersect`

`GeoWorkload` requires a `Memcached` instance to sample values from the storage-based generator 
by fetching pre-generated values/documents from an internal in-memory database instead of 
generating new random values on the fly. It could be configured in the workload settings:

* `geo_storage_host`
* `geo_storage_port`

Database client has to be extended with geo operations. Currently, MongoDB, Couchbase, and ArangoDB are extended. 
Other databases could be adapted in the same way.

For example, to run a geo benchmark on the MongoDB:

1. Create MongoDB database and populate a collection (`usertable` was defined in YCSB, it is also kept in GeoYCSB) with GeoJSON values.
Create geospatial index on the GeoJSON field.

2. Load the benchmark:
``` shell
bin/ycsb.sh load mongodb -P workloads/workloada -p mongodb.url="mongodb://localhost:27017/ycsb?w=1" -p mongodb.auth="true"
```

3. Run the benchmark:
``` shell
bin/ycsb.sh run mongodb -P workloads/workloada -p mongodb.url="mongodb://localhost:27017/ycsb?w=1" -p mongodb.auth="true"
```


Building from source
--------------------

YCSB requires the use of Maven 3; if you use Maven 2, you may see [errors
such as these](https://github.com/brianfrankcooper/YCSB/issues/406).

To build the full distribution, with all database bindings:

    mvn clean package

To build a single database binding:

    mvn -pl site.ycsb:mongodb-binding -am clean package
