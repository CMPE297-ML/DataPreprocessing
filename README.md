# Data Preprocessing
Repository for ML Project of CMPE297


##Usage

1. Install cassandraDB.
2. Clone this repository.
3. Install dependencies
...```
npm install
```
4. Start file processing. Carefully, provide absolute path to the files. *(In the example below the files are in ...daily-files folder.)*
...
```
node procFiles.js /Users/gauravchodwadia/Desktop/daily-files/
```
5. Start calculating the averages and store in the DB
...
```
node calcAvg.js
```
