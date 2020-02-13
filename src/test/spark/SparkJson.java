package test.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJson {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("json tests").master("local").getOrCreate();
		Dataset<Row> transDS =  spark.read().option("multiLine", true).format("json").load("resources/poslogu.json");
		transDS.show();
		transDS.select("LINES").show();
		

	}
	
}
