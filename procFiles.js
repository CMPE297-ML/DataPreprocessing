var cassandra = require('cassandra-driver');

var distance = cassandra.types.distance;
var fs = require('fs');
var dbURL = 'localhost:9042';
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
  var lineReader = require('readline').createInterface({
    input: fs.createReadStream(filePath + file)
  });
  var count = 0;
  lineReader.on('line', function (line) {
    count++;
    var data = line.split(',');
	if(data)
	{	
		//console.log(data);	
		
    var permno = data[0];

    //console.log(data[1] +"=="+ file +"=="+ count );
    if(data[1] === undefined){
      return;
    }

    var date = new Date(data[1].substring(0,4) + "/" + data[1].substring(4,6) + "/" + data[1].substring(6,8)).getTime();
    var price = data[46];
    var priceInt;

    if(price == ''){
      priceInt = 0;
    }
    else{
      priceInt= Math.abs(parseFloat(price));
    }

    var queryString = "insert into stock_data (permno,date,price) values ("+permno+",'"+date+"',"+priceInt+");";
    client.execute(queryString, function (error, result) {
        if (!error){
        }else{
          console.log(error);
        }
    });
	}
  });
};
