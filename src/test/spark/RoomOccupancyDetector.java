package test.spark;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.spark_project.jpmml.model.visitors.IntegerInterner;

public class RoomOccupancyDetector {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// Create Spark Session to create connection to Spark
		final SparkSession sparkSession = SparkSession.builder().appName("Spark Decision Tree Classifer Demo")
				.master("local").getOrCreate();

		
		
		// Get DataFrameReader using SparkSession and set header option to true
		// to specify that first row in file contains name of columns
		final DataFrameReader dataFrameReader = sparkSession.read().option("header", true);
		
		final Dataset<Row> trainingData = dataFrameReader.csv("resources/datatraining.txt");
		
		
	
		// Create view and execute query to convert types as, by default, all
		// columns have string types
		trainingData.createOrReplaceTempView("TRAINING_DATA");
		final Dataset<Row> typedTrainingData = sparkSession
				.sql("SELECT cast(Temperature as float) Temperature, cast(Humidity as float) Humidity, "
						+ "cast(Light as float) Light, cast(CO2 as float) CO2, "
						+ "cast(HumidityRatio as float) HumidityRatio, "
						+ "cast(Occupancy as int) Occupancy FROM TRAINING_DATA");

		// Combine multiple input columns to a Vector using Vector Assembler
		// utility
		final VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] { "Temperature", "Humidity", "Light", "CO2", "HumidityRatio" })
				.setOutputCol("features");
		final Dataset<Row> featuresData = vectorAssembler.transform(typedTrainingData);
		
		
		
		// Print Schema to see column names, types and other metadata
		featuresData.printSchema();

		// Indexing is done to improve the execution times as comparing indexes
		// is much cheaper than comparing strings/floats

		// Index labels, adding metadata to the label column (Occupancy). Fit on
		// whole dataset to include all labels in index.
		
		//final StringIndexerModel labelIndexer = new StringIndexer().setInputCol("Occupancy").setOutputCol("indexedLabel").fit(featuresData);
		// Index features vector
		//final VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(featuresData);

		
		// Split the data into training and test sets (30% held out for
		// testing).
		Dataset<Row>[] splits = featuresData.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> trainingFeaturesData = splits[0];
		Dataset<Row> testFeaturesData = splits[1];

		// Train a DecisionTree model.
		//final DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures");
		
		final DecisionTreeClassifier dtunindexed = new DecisionTreeClassifier().setLabelCol("Occupancy").setFeaturesCol("features");
		
		

		
		// Convert indexed labels back to original labels.
		//final IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedOccupancy").setLabels(labelIndexer.labels());

		// Chain indexers and tree in a Pipeline.
		//final Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { labelIndexer, featureIndexer, dt, labelConverter });
		
		final Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { dtunindexed });

		
		// Train model. This also runs the indexers.
		final PipelineModel model = pipeline.fit(trainingFeaturesData);

		// Make predictions.
		final Dataset<Row> predictions = model.transform(testFeaturesData);
		
		//model.save("resources/modelsaved");
		//model.write().save("resources/modelsaved/");

		// Select example rows to display.
		//predictions.show();
		//System.out.println("Example records with Predicted Occupancy as 0:");
		//predictions.select("prediction", "Occupancy", "features").where(predictions.col("prediction").equalTo(0)).show(10);
//
//		System.out.println("Example records with Predicted Occupancy as 1:");
//		predictions.select("predictedOccupancy", "Occupancy", "features")
//				.where(predictions.col("predictedOccupancy").equalTo(1)).show(10);
//
//		System.out.println("Example records with In-correct predictions:");
//		predictions.select("predictedOccupancy", "Occupancy", "features")
//				.where(predictions.col("predictedOccupancy").notEqual(predictions.col("Occupancy"))).show(10);
	}

}