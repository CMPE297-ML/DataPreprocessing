import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Stocks {
		static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost/nystocks";
	public static void main(String args[]){
		Stocks ob = new Stocks();
		
		try (BufferedReader br = new BufferedReader(new FileReader("/Users/ekta2803/Desktop/data.csv")))
		{
			Class.forName(JDBC_DRIVER);
			Connection con=DriverManager.getConnection(DB_URL,"root","root"); 
			String scurrentLine;
			int count = 0;
			StringBuffer qry = new StringBuffer();
			Statement stmt=con.createStatement();
			while((scurrentLine = br.readLine()) != null)
			{
				
				if(!scurrentLine.equals("")){
					
					String tokens[] = scurrentLine.split(",",-1);
					String query = "INSERT INTO stock_tab (CUSIP,PERMNO,PERMCO,ISSUNO,HEXCD,HSICCD,date,BIDLO,ASKHI,PRC,VOL,RET,BID,ASK,SHROUT,CFACPR,CFACSHR,ALTPRC,SPREAD,ALTPRCDT,RETX)"
							+ "values('"+tokens[0]+"','"+tokens[1]+"','"+tokens[2]+"','"+tokens[3]+"','"+tokens[4]+"','"+tokens[5]+"','"+tokens[6]+"','"+tokens[7]+"','"+tokens[8]+"','"+tokens[9]+"'"
									+ ",'"+tokens[10]+"','"+tokens[11]+"','"+tokens[12]+"','"+tokens[13]+"','"+tokens[14]+"','"+tokens[15]+"','"+tokens[16]+"','"+tokens[17]+"','"+tokens[18]+"'"
											+ ",'"+tokens[19]+"','"+tokens[20]+"');";
					stmt.execute(query);
				}
				System.out.println(count++);
			}
			stmt.close();
			con.close();	
		}catch(Exception e){
			e.printStackTrace();	
		}
		
	}
}
