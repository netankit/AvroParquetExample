AvroParquetExample
==================

@Author: Ankit Bahuguna

@Date: September 5, 2014


Description: This project demonstrates usage of Parquet and Avro. Basically, I am trying to investigate how the avro objects/ schema can be used to read write parquet files. There are specific use cases explored such as generic parquet compaction using single thread mechanism and also a map reduce based version for utilizing hadoop framework for parallelization.

Some more information about Avro and Parquet can be found below:

Avro
====

Apache Avro™ is a data serialization system.

Avro provides:

* Rich data structures. A compact, fast, binary data format. 
* A container file, to store persistent data. Remote procedure call (RPC). 
* Simple integration with dynamic languages. 
* Code generation is not required to read or write data files nor to use or implement RPC protocols. 
* Code generation as an optional optimization, only worth implementing for statically typed languages. 

Link: http://avro.apache.org/

Parquet
=======

Apache Parquet is a columnar storage format available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language.

Link: http://parquet.incubator.apache.org/

