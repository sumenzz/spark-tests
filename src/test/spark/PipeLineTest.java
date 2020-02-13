package test.spark;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import  org.apache.spark.sql.functions.*;

import breeze.linalg.max;

public class PipeLineTest {
	
	
	public static void main(String[] args) {
		
		List<Row> data = Arrays.asList(
				RowFactory.create(0, "hi its me sumen",new Timestamp(System.currentTimeMillis())),
				RowFactory.create(1, "hi its me gracy",new Timestamp(System.currentTimeMillis())),
				RowFactory.create(2, "hi its me jeremy",new Timestamp(System.currentTimeMillis()))
				);	
		
		
	
		StructType struct = new StructType( new StructField[] {
				new StructField("rowid",DataTypes.IntegerType,false,Metadata.empty()),
				new StructField("sentence",DataTypes.StringType,false,Metadata.empty()),
				new StructField("date",DataTypes.TimestampType,false,Metadata.empty())
				}

			);
		
		
		SparkSession spark = SparkSession.builder().master("local").appName("Test").getOrCreate();
		
		Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, struct);
		
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words").setPattern("\\W");
		
		Dataset<Row> tokenized = tokenizer.transform(sentenceDataFrame);
		Dataset<Row> regexTokenized = regexTokenizer.transform(sentenceDataFrame);
		
		//tokenized.show();
		//regexTokenized.show();
		
		
		sentenceDataFrame.createOrReplaceTempView("test");
		
		//spark.sql("select sum(rowid) over (partition by 1 ) from test").show();
		
		//spark.sql("select level connect by level < 10").show();;
		//spark.sql("select last_value(rowid) over (partition by 1) from test").show();
		sentenceDataFrame.agg(functions.max(sentenceDataFrame.col("rowid"))).show();
		
		
		
		
	}		
		
		
		
		
		
		
				
				
				
				
	
	 

}
