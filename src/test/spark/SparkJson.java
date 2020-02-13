package test.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkJson {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("json tests").master("local").getOrCreate();
		Dataset<Row> transDS =  spark.read().option("multiLine", true).format("json").load("resources/poslogu.json");
		
		//transDS.show(); // show tables
		//transDS.select("LINES.LINEITEMS").show(false); // selecting nested json fields

		// creating transaction line table (using explode)
		Dataset<Row> lineitems = transDS.select(transDS.col("RETAILSTOREID"),transDS.col("BUSINESSDAYDATE"),transDS.col("TRANSACTIONTYPECODE"),transDS.col("TRANSACTIONSEQUENCENUMBER"),org.apache.spark.sql.functions.explode(transDS.col("LINES.LINEITEMS"))).toDF().alias("test");
		lineitems.select("test.*","test.col.LINENUMBER").show(false);
		
		//transDS.select(org.apache.spark.sql.functions.get_json_object("RETAILSTOREID","$..RETAILSTOREID")).show();
		
		
		//"RETAILSTOREID","BUSINESSDAYDATE","TRANSACTIONTYPECODE","WORKSTATIONID","TRANSACTIONSEQUENCENUMBER"
		

	}
	
}
