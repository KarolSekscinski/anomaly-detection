import org.apache.spark.sql.{DataFrame}
import org.apache.spark.ml.feature.VectorAssembler
import com.linkedin.relevance.isolationforest.IsolationForest

object IsolationForestWrapper {

  /**
   * Function to apply Isolation Forest to detect anomalies.
   *
   * @param df Input DataFrame
   * @param featureCols Array of feature column names
   * @param contamination Fraction of anomalies in the data
   * @return DataFrame with outlier scores and predicted labels
   */
  def detectAnomalies(df: DataFrame, featureCols: Array[String], contamination: Double): DataFrame = {
    import org.apache.spark.sql.functions.col

    // Define feature column for assembled vector
    val featuresCol = "features"

    // Assemble features into a single vector
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol(featuresCol)
    val assembledData = assembler.transform(df)

    // Initialize Isolation Forest model
    val isolationForest = new IsolationForest()
      .setNumEstimators(100) // Number of trees in the ensemble
      .setBootstrap(false) // Use sampling without replacement
      .setMaxSamples(256) // Number of samples per tree
      .setMaxFeatures(1.0) // Use all features
      .setFeaturesCol(featuresCol) // Column containing feature vectors
      .setPredictionCol("predictedLabel") // Column for predicted anomaly labels
      .setScoreCol("outlierScore") // Column for anomaly scores
      .setContamination(contamination) // Expected fraction of anomalies
      .setContaminationError(0.01 * contamination) // Allowable contamination error
      .setRandomSeed(1) // Seed for reproducibility

    // Train the model and transform data
    val isolationForestModel = isolationForest.fit(assembledData)
    val dataWithScores = isolationForestModel.transform(assembledData)

    // Return DataFrame with outlier scores and predicted labels
    dataWithScores
      .select("features", "outlierScore", "predictedLabel") // Adjust columns as needed
  }
}
