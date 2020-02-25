package test.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Encoders;


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
					new StructField("RETAILSTOREID", DataTypes.IntegerType, false, Metadata.empty()),
				    new StructField("BUSINESSDAYDATE", DataTypes.IntegerType, false, Metadata.empty()),
				    new StructField("TRANSACTIONTYPECODE", DataTypes.IntegerType, false, Metadata.empty()),
				    new StructField("WORKSTATIONID", DataTypes.IntegerType, false, Metadata.empty()),
				    new StructField("TRANSACTIONSEQUENCENUMBER", DataTypes.IntegerType, false, Metadata.empty()),
				    new StructField("DATA", DataTypes.StringType, false, Metadata.empty())
				   });
			//schema.printTreeString();
			
			Dataset<Row> transDS =  spark.read().schema(schema).option("multiLine", true).json("resources/poslogu1.json");
			Dataset<Row> custDS =  spark.read().option("multiLine", true).json("resources/customer.json");
			
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
			//running totalW
			//transDS.withColumn("test", org.apache.spark.sql.functions.sum(transDS.col("WORKSTATIONID")).over(Window.partitionBy("BUSINESSDAYDATE").orderBy("WORKSTATIONID").rowsBetween(1, 2))).show();
			//transDS.select("BUSINESSDAYDATE")
			//transDS.select(org.apache.spark.sql.functions.sum(transDS.col("WORKSTATIONID")).over(Window.partitionBy("BUSINESSDAYDATE").orderBy("WORKSTATIONID"))).show();
			
			
			//lamdba
//			Encoder<String> encoder = Encoders.STRING();
//			transDS.map( (MapFunction<Row, String>) row -> "Business date " + Integer.toString(row.getInt(0)),encoder).show(false);
//			//transDS.select("BUSINESSDAYDATE").map((MapFunction<Row, String>) row -> "Business date " + Integer.toString(row.getAs("BUSINESSDAYDATE")),encoder).show(false);
			
			//anonymous function
			//transDS.select("BUSINESSDAYDATE").map(new MapFunction<Row,String>() {
				
			// public String call(Row value) throws Exception {
			//  return "Business date " + Integer.toString(value.getAs("BUSINESSDAYDATE"));
			// }					
			// },encoder).show(false);
			
			
			
			//anonymous function with custom encoder
//			Encoder<SampleDataMapper> dataMapper =Encoders.bean(SampleDataMapper.class);
//			Encoder<String> encoder = Encoders.STRING();
//			Dataset<SampleDataMapper> dataMap = transDS.map(new MapFunction<Row,SampleDataMapper>() {
//
//				public SampleDataMapper call(Row value) throws Exception {
//					String date = Integer.toString(value.getAs("BUSINESSDAYDATE"));
//					SampleDataMapper mappedD = new SampleDataMapper(date);
//					return mappedD;
//				}					
//						
//			},dataMapper);
//			dataMap.show(false);
			//dataMap.map((MapFunction<SampleDataMapper, String>) row -> row.modify(),encoder).show(false);
			
			//Encoder<Iterator<String>> encoder = Encoders.class<Encoders.STRING();
			//transDS.flatMap( (FlatMapFunction<Row, Iterable<String>>) row -> Arrays.asList(row.getAs("DATA").toString().split(",")),encoder).show(false);
			
			//joins
			//custDS.show();
			//transDS.join(custDS, "RETAILSTOREID").show();
//			transDS.join(custDS, 
//			transDS.col("RETAILSTOREID").equalTo(custDS.col("RETAILSTOREID")).and(transDS.col("BUSINESSDAYDATE").equalTo(custDS.col("BUSINESSDAYDATE"))).and(transDS.col("TRANSACTIONTYPECODE").equalTo(custDS.col("TRANSACTIONTYPECODE")))
//			.and(transDS.col("WORKSTATIONID").equalTo(custDS.col("WORKSTATIONID"))).and(transDS.col("TRANSACTIONSEQUENCENUMBER").equalTo(custDS.col("TRANSACTIONSEQUENCENUMBER"))),"LeftOuter"
//			).show();
			
			//transDS.filter(transDS.col("RETAILSTOREID").like("2020")).show();
			
//			for(int i=1 ; i < transDS.count() + 1;i++)
//				System.out.println(transDS.getRows(i,0).toString());
				
			
			
			
			
			

	}
	
	
	
}
