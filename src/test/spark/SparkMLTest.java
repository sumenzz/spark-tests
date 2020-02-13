package test.spark;


import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

public class SparkMLTest {

	
	 public static class Rating implements Serializable {
		    private int userId;
		    private int movieId;
		    private float rating;
		    private long timestamp;

		    public Rating() {}

		    public Rating(int userId, int movieId, float rating, long timestamp) {
		      this.userId = userId;
		      this.movieId = movieId;
		      this.rating = rating;
		      this.timestamp = timestamp;
		    }
		    
		    public int getUserId() {
		        return userId;
		      }

		      public int getMovieId() {
		        return movieId;
		      }

		      public float getRating() {
		        return rating;
		      }

		      public long getTimestamp() {
		        return timestamp;
		      }
		    
		      public static Rating parseRating(String str) {
		          String[] fields = str.split("::");
		          if (fields.length != 4) {
		            throw new IllegalArgumentException("Each line must contain 4 fields");
		          }
		          int userId = Integer.parseInt(fields[0]);
		          int movieId = Integer.parseInt(fields[1]);
		          float rating = Float.parseFloat(fields[2]);
		          long timestamp = Long.parseLong(fields[3]);
		          return new Rating(userId, movieId, rating, timestamp);
		        }
		      
	 		}
	 
	 
	 public static void main(String[] args) throws AnalysisException {
		 
		 SparkSession spark = SparkSession.builder().master("local").appName("SparkMLTest").getOrCreate();
		 JavaRDD<Rating> ratingsRDD = spark.read().textFile("resources/sample_movielens_ratings.txt").javaRDD().map(Rating::parseRating);
		 //Dataset<Row> df = spark.read().format("com.databricks.spark.csv").option("delimiter", "||").load("C:\\\\Users\\\\smaibam\\\\Desktop\\\\sample_movielens_ratings.txt");
		 Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
		 //ratings.show(100);;
		 Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
		 Dataset<Row> training = splits[0];
		 Dataset<Row> test = splits[1];
		 training.createOrReplaceTempView("training");
		 spark.sql("select * from training").show();
		 
		 //spark.sql("select distinct movieId from training ").show();
		 //spark.sql("select distinct userId from training minus select distinct userId from training where movieId=31").show();
		
		 //training.select("userId","movieId").limit(10).show();
		 
		 
		// MLUtils.saveAsLibSVMFile(training.rdd().m, "c://Desktop//");
		 
		 
		 
		 
//		 ALS als = new ALS()
//				  .setMaxIter(5)
//				  .setRegParam(0.01)
//				  .setUserCol("userId")
//				  .setItemCol("movieId")
//				  .setRatingCol("rating");
//		 
//		 
//		ALSModel model = als.fit(training);
//		 
//		model.setColdStartStrategy("drop");
//		
//		Dataset<Row> predictions = model.transform(test);
//		
//		predictions.describe("rating").show();	
		
	

//		Long rec = predictions.count();
//		Long rec1 = training.count();
//		System.out.println(rec);
//		System.out.println(rec1);

//		RegressionEvaluator evaluator = new RegressionEvaluator()
//				  .setMetricName("rmse")
//				  .setLabelCol("rating")
//				  .setPredictionCol("prediction");
//				Double rmse = evaluator.evaluate(predictions);
//				System.out.println("Root-mean-square error = " + rmse);
				
//				//Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);
//				//Dataset<Row> users = spark.sql("select userId from training where userId=28")
				
//				StructField[] StructField = new StructField[]{
//			            new StructField("userId", DataTypes.IntegerType, true, Metadata.empty()),
//			            new StructField("test", DataTypes.IntegerType, true, Metadata.empty())
//			    };
//				
//				StructType schema = new StructType(StructField);
//				
//				List <Row> rows = new ArrayList<>(); 
//				rows.add(RowFactory.create(28,1));
//				Dataset<Row> user = spark.createDataFrame(rows, schema);
//				
//				
//				
//				//Dataset<Row> users = spark.sql("select distinct userId from training where userId=28");
//				Dataset<Row> userSubsetRecs = model.recommendForUserSubset(user.select("userId"), 1);
//				userSubsetRecs.show();
//				
//				
//				
		
		 
	 }
}

