var cassandra = require('cassandra-driver');
var dbURL = '127.0.0.1';
var client = new cassandra.Client({contactPoints: [dbURL], keyspace: 'nystock'});
var permno =[];
var queryString = "select distinct permno from stock_data";

	var count = 0;
	client.execute(queryString, function (error, result) {

      if (!error){	
      	result.rows.forEach(function(row){
      		permno.push(row);
      	});
  	console.log("---->>>>");
      for(no in permno){
	      var qry = "select * from stock_data where permno = "+permno[no].permno+" allow filtering;";
	      
	      client.execute(qry, function (error, res) {
	      	if (!error){	
      		//console.log(res.rows);
      			var prevPrice;
      			for(var i = 0;i<res.rows.length;i++){
      			var avgPrc;

      			if(i == 0){
      				avgPrc = 0;
      			}
      			else{
      				avgPrc = (parseFloat(res.rows[i].price)/prevPrice)-1;
      			}
      			console.log(avgPrc);
      			var qry = "update stock_data set avg_price = "+avgPrc+" where permno = "+parseInt(res.rows[i].permno)+" and date = "+new Date(res.rows[i].date).getTime()+" and price = "+parseFloat(res.rows[i].price)+";";
      			//console.log(qry);
      			prevPrice = parseFloat(res.rows[i].price);
      			client.execute(qry, function (error, result) {
					if (!error){	
      					console.log(result);
      					
      				}else{
      					console.log("error");
 	     			}
      			});
      		}
      }
      else{
      	console.log(error);
      }
    });
    }  
  }else{
      	console.log(error);
      }
      
 	});
