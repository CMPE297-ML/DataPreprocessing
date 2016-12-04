# Data Preprocessing
Repository for ML Project of CMPE297


##Usage

1. Install cassandraDB.
2. Create keyspace and table

  ```
  CREATE KEYSPACE nystock WITH replication = {'class' : 'SimpleStrategy', replication_factor' : 2};
  Create Table nystock.stock_data (
     permno int,
     date timestamp,
     price float,
     avg_price float,
     PRIMARY KEY(permno, date,price))
     WITH CLUSTERING ORDER BY (date ASC)
     AND COMPRESSION = { 'sstable_compression' : 'SnappyCompressor'} ;
  ```
3. Clone this repository.
4. Install dependencies

  ```
npm install
```
5. Start file processing. Carefully, provide absolute path to the files. *(In the example below the files are in daily-files folder.)*

  ```
node procFiles.js /Users/gauravchodwadia/Desktop/daily-files/
```
6. Start calculating the averages and store in the DB (Not fully tested yet. May have bugs.)

  ```
node calcAvg.js
```
