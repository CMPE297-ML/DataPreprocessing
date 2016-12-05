var cassandra = require('cassandra-driver');
var fs = require('fs');
var dbURL = '127.0.0.1';
var client = new cassandra.Client({contactPoints: [dbURL], keyspace: 'nystock'});

const path = './74d15d2fb10f941d.csv';
fs.readFile(path, 'utf8',function(err, file){
  if(err){
    console.log("Error in reading daily-files: " + err);
  }else{
    console.log("Processing...!!");
    
      processFile(file);
    
  }
});


var processFile = function(file){
  var lineReader = require('readline').createInterface({
    input: fs.createReadStream(path)
  });
   var queries = [];
   var count = 0;
   var errorInt = 0;
  	lineReader.on('line', function (line,lineCount) {
   		var data = line.split(',');
    	var permno = data[1];
    	var date = new Date(data[6].substring(0,4) + "/" + data[6].substring(4,6) + "/" + data[6].substring(6,8)).getTime();
    	var price = data[9];
    	var priceInt;

    	if(price == ''){
      		priceInt = 0;
    	}
    	else{
      		priceInt= Math.abs(parseFloat(price));
    	}

    	var queryString = "insert into stock (permno,date,price) values (?,?,?);";
    /*client.execute(queryString, function (error, result) {
        if (!error){
        }else{
          //console.log(error);
        }
    });*/if(count!=0)
 	   		queries.push({"query" : queryString,"params":[permno,date,priceInt]});
    	count++;
    	if(count==2000)
    	{
    		count=0;

      		//console.log(JSON.stringify(queries));
      		client.batch(queries, { prepare: true }, function (err) {
   // All queries have been executed successfully
   // Or none of the changes have been applied, check err
   			if(err)
   			{
    			console.log("error : "+err);
    			console.log(errorInt++);

   			}
   			else
   			{
   				
    			console.log("data inserted");
  		 	}
  		 	queries = [];
   			});
  		}
	});
  }


