import LshEuclidean.spark
import org.apache.spark.ml.linalg.Vectors

object LSHmodel {
  def usingRdd(pathF1:String,pathF2:String){
    val frame1=spark.read.parquet(pathF1)
    val keys = Seq(Vectors.dense(1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0),
      Vectors.dense(0.04576,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435),
      Vectors.dense(0.1230,0.3450,0.3450,0.3450,0.3450,0.3450,0.3450,0.3450,0.3450,0.3450),
      Vectors.dense(0.2340,0.045,0.045,0.045,0.045,0.045,0.045,0.045,0.045,0.045),
      Vectors.dense(0.3450,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0),
      Vectors.dense(0.450,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435),
      Vectors.dense(0.7680,0.25430,0.25430,0.25430,0.25430,0.25430,0.25430,0.25430,0.25430,0.25430),
      Vectors.dense(0.2340,0.0564,0.0564,0.0564,0.0564,0.0564,0.0564,0.0564,0.0564,0.0564),
      Vectors.dense(0.8770,0.0212,0.0212,0.0212,0.0212,0.0212,0.0212,0.0212,0.0212,0.0212),
      Vectors.dense(0.456210,0.2430,0.2430,0.2430,0.2430,0.2430,0.2430,0.2430,0.2430,0.2430)
    )
    val preeTrainedModel=LshEuclidean.getModel(pathF1,pathF2)
    val keysrdd=spark.sparkContext.parallelize(keys)

    val results=keysrdd.map(x=>{
      var y=0
      y+1
      println(y)
      val arr=preeTrainedModel.approxNearestNeighbors(frame1,x,10).write.parquet("/home/rochan/Sparkout/modelout"+y)
      preeTrainedModel
    })
  }
}