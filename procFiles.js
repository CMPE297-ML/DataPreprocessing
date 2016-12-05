var cassandra = require('cassandra-driver');

var distance = cassandra.types.distance;
var fs = require('fs');
var dbURL = '127.0.0.1';
var client = new cassandra.Client({contactPoints: [dbURL],pooling: {
        coreConnectionsPerHost: {
          [distance.local] : 250
        }
        } , keyspace: 'nystock'});

var filePath = process.argv[2];

//------------Read all the files in daily-files-----------------//

fs.readdir(filePath, function(err, files){
  if(err){
    console.log("Error in reading daily-files: " + err);
  }else{
    console.log("Processing...!!");
    files.forEach(function(file){
      if(file.split(".").pop() == "csv")
      processFile(file);
    });
  }
});


var processFile = function(file){
  // var lineReader = require('linebyline').createInterface({
  //   input: fs.createReadStream(filePath + file)
  // });
  var readline = require('linebyline'),
  lineReader = readline(filePath + file, {
    retainBuffer: false //tell readline to retain buffer  
  });

  var queries = [];
  lineReader.on('line', function (line,lineCount) {
    var data = line.split(',');
    var permno = data[0];
    var date = new Date(data[1].substring(0,4) + "/" + data[1].substring(4,6) + "/" + data[1].substring(6,8)).getTime();
    var price = data[46];
    var priceInt;

    if(price == ''){
      priceInt = 0;
    }
    else{
      priceInt= Math.abs(parseFloat(price));
    }

    var queryString = "insert into stock_data (permno,date,price) values (?,?,?);";
    /*client.execute(queryString, function (error, result) {
        if (!error){
        }else{
          //console.log(error);
        }
    });*/
    queries.push({"query" : queryString,"params":[permno,date,priceInt]});

    if(queries.length==1000)
    {
      console.log(JSON.stringify(queries));
      client.batch(queries, { prepare: true }, function (err) {
   // All queries have been executed successfully
   // Or none of the changes have been applied, check err
   if(err)
   {
    console.log("error : "+err);
   }
   else
   {
    console.log("data inserted");
   }
   });
    }
  });

/*
  client.batch(queries, { prepare: true }, function (err) {
   // All queries have been executed successfully
   // Or none of the changes have been applied, check err
   if(err)
   {
    console.log("error : "+error);
   }
   else
   {
    console.log("data inserted");
   }

});
*/

}