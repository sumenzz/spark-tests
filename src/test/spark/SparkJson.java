package test.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;


public class SparkJson {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("json tests").master("local").getOrCreate();
		//Dataset<Row> transDS =  spark.read().option("multiLine", true).format("json").load("resources/poslogu.json");
		
		//transDS.show(); // show tables
		//transDS.select("LINES.LINEITEMS").show(false); // selecting nested json fields

		// creating transaction line table (using explode)
		
			//Dataset<Row> lineitems = transDS.select(transDS.col("RETAILSTOREID"),transDS.col("BUSINESSDAYDATE"),transDS.col("TRANSACTIONTYPECODE"),transDS.col("TRANSACTIONSEQUENCENUMBER"),org.apache.spark.sql.functions.explode(transDS.col("LINES.LINEITEMS"))).toDF().alias("test");
			//lineitems.select("test.*","test.col.LINENUMBER").show(false);

		// creating transaction line using custom schema
			
			StructType schema = new StructType(new StructField[]{
				    new StructField("BUSINESSDAYDATE", DataTypes.IntegerType, false, Metadata.empty()),
				    new StructField("TRANSACTIONTYPECODE", DataTypes.IntegerType, false, Metadata.empty()),
				    new StructField("WORKSTATIONID", DataTypes.IntegerType, false, Metadata.empty()),
				    new StructField("TRANSACTIONSEQUENCENUMBER", DataTypes.IntegerType, false, Metadata.empty())
				   });
			schema.printTreeString();
			
			Dataset<Row> transDS =  spark.read().schema(schema).option("multiLine", true).json("resources/poslogu1.json");
			
		//operations
			//multiply
			//transDS.select(transDS.col("WORKSTATIONID").multiply(transDS.col("TRANSACTIONSEQUENCENUMBER"))).show();
			//divide
			//transDS.select(transDS.col("WORKSTATIONID").divide(transDS.col("TRANSACTIONSEQUENCENUMBER"))).show();
			//add
			//transDS.select(transDS.col("WORKSTATIONID").plus((transDS.col("TRANSACTIONSEQUENCENUMBER")))).show();
			//minus
			//transDS.select(transDS.col("WORKSTATIONID").minus((transDS.col("TRANSACTIONSEQUENCENUMBER")))).show();
			//agg
			//transDS.select(org.apache.spark.sql.functions.sum(transDS.col("WORKSTATIONID") )).show();
			//transDS.groupBy(transDS.col("BUSINESSDAYDATE")).agg(org.apache.spark.sql.functions.sum(transDS.col("WORKSTATIONID"))).show();
			//order by
			//transDS.select("BUSINESSDAYDATE").orderBy(org.apache.spark.sql.functions.desc("BUSINESSDAYDATE")).show();
			//running total
			transDS.withColumn("test", org.apache.spark.sql.functions.sum(transDS.col("WORKSTATIONID")).over(Window.partitionBy("BUSINESSDAYDATE").orderBy("WORKSTATIONID"))).show();

			
			
			
		
		

	}
	
}
