
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel}
import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.sql.functions.col

import scala.util.Random

object LshEuclidean extends Context{

  def getModel(frame1Path:String,frame2Path:String): BucketedRandomProjectionLSHModel={

    // data was generated using random function
/*
      val data1= Seq.fill(100)(Random.nextDouble(),Vectors.dense(Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble()))
      val data2= Seq.fill(100)(Random.nextDouble(),Vectors.dense(Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble(),Random.nextDouble()))
      val frame1 = spark.createDataFrame(data1).toDF("id","features")
      val frame2 = spark.createDataFrame(data2).toDF("id","features")
      frame1.write.parquet("path to save frame2")
      frame2.write.parquet("path to save frame2")
*/

    val frame1=spark.read.parquet(frame1Path)
    val frame2=spark.read.parquet(frame2Path)

     val brp = new BucketedRandomProjectionLSH()
        .setBucketLength(4.0)
        .setNumHashTables(20)
        .setInputCol("features")
        .setOutputCol("hashes")

      val model = brp.fit(frame1)
      model.approxSimilarityJoin(frame1, frame2, 1.5, "EuclideanDistance")
        .select(col("datasetA.id").alias("idA"),
          col("datasetB.id").alias("idB"),
          col("EuclideanDistance"))

      model

  }
}
