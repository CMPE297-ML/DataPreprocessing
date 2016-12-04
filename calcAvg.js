"use strict";

var cassandra = require('cassandra-driver');
var dbURL = '127.0.0.1';
var client = new cassandra.Client({contactPoints: [dbURL], keyspace: 'nystock'});
var permno =[];
var queryString = "select distinct permno from stock_data";
	console.log("Processing..!");
	var count = 0;
	client.execute(queryString, function (error, result) {
		if (!error){
			result.rows.forEach(function(row){
				permno.push(row.permno);
			});
			console.log("---->>>>"+permno.length);
			permno.forEach(function(no){
				var query = "select * from stock_data where permno = "+no+" allow filtering;";
				const options = {autoPage: true, fetchSize: 10000};
						client.execute(query,[],options, function (error, res) {
							if (!error){
								console.log("query :"+query);
								console.log("length: "+res.rows.length);
								for(var i = 0;i<res.rows.length;i++)
								{
									calculateAverage(i,res);
								}
							}
							else{
								console.log(error);
							}
						});
			});
				
			
		}else{
			console.log(error);
		}

	});


	function calculateAverage(i,res)
	{
		//console.log("i"+i+"::"+res.rows.length);
		if(i==res.rows.length-1)
		{
			console.log(i);
		}
							var avgPrc =0;

							if(i != 0)
							{
								
								if(parseFloat(res.rows[i-1].price)!=0)
								{
									avgPrc = (parseFloat(res.rows[i].price)/parseFloat(res.rows[i-1].price))-1;
								}
								else
								{
									avgPrc = parseFloat(res.rows[i-1].price);
								}
							}
							//console.log(avgPrc);
							var qry = "update stock_data set avg_price = "+avgPrc+" where permno = "
								+parseInt(res.rows[i].permno)+" and date = "+new Date(res.rows[i].date).getTime()
								+" and price = "+parseFloat(res.rows[i].price)+";";
							console.log(res.rows[i].date);	
							client.execute(qry, function (error, result) {
								if (!error){
									//console.log(result);
								}else{
									console.log(error);
								}
							});
	}
