var cassandra = require('cassandra-driver');
var fs = require('fs');
var dbURL = '127.0.0.1';
var client = new cassandra.Client({contactPoints: [dbURL], keyspace: 'nystock'});

var lineReader = require('readline').createInterface({
  input: require('fs').createReadStream('./file1.csv')
});
var count = 0;
lineReader.on('line', function (line) {
 var data = line.split(',');
  var permno = data[0];
  var date = data[1].split("");
  var dateFormat = date[0]+date[1]+date[2]+date[3]+"/"+date[4]+date[5]+"/"+date[6]+date[7];
  var price = data[46];
  var priceInt;
  if(price == ''){
  	priceInt = 0;
  }
  else{
  	priceInt= Math.abs(parseFloat(price));
  }
   
 
  var queryString = "insert into stock_data (permno,date,price) values ("+permno+",'"+dateFormat+"',"+priceInt+");";
	client.execute(queryString, function (error, result) {

      if (!error){
      	console.log(count++);
      }else{
      	console.log(error);
      }
 	});
});


/*fs.readFile('./file1.csv', 'utf8', function (err,data) {
  if (err) {
    return console.log(err);
  }
  var data = data.split(',');
  var permno = data[0];
  var date = data[1];
  var price = data[46];
  console.log(permno);
});*/

    

	console.log("outside callback");

