import java.util

import scala.collection.mutable.ArrayBuffer

//import Verify.{entries, frame1}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql

import math.{sqrt, _}
object Verify extends Context {

  def checkSimilarity(pathF1:String){
    val frame1= spark.read.parquet(pathF1)
    val  entries = Array(Array(1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0),
      Array(0.04576,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435),
      Array(0.1230,0.3450,0.3450,0.3450,0.3450,0.3450,0.3450,0.3450,0.3450,0.3450),
      Array(0.2340,0.045,0.045,0.045,0.045,0.045,0.045,0.045,0.045,0.045),
      Array(0.3450,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0),
      Array(0.450,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435,0.0435),
      Array(0.7680,0.25430,0.25430,0.25430,0.25430,0.25430,0.25430,0.25430,0.25430,0.25430),
      Array(0.2340,0.0564,0.0564,0.0564,0.0564,0.0564,0.0564,0.0564,0.0564,0.0564),
      Array(0.8770,0.0212,0.0212,0.0212,0.0212,0.0212,0.0212,0.0212,0.0212,0.0212),
      Array(0.456210,0.2430,0.2430,0.2430,0.2430,0.2430,0.2430,0.2430,0.2430,0.2430)
    )
    import spark.implicits._
    val manualout= entries.map(entryA=>{
      val feature = frame1.select($"features").map(x => x.getAs[org.apache.spark.ml.linalg.DenseVector](0).values).collect()

      (entryA.mkString(","),feature.map(entryB => {
        (sqrt(entryA.zipWithIndex.map(i => {
          val t = entryB(i._2) - i._1;
          t * t
        }).sum), entryB.mkString(","))
      }
      ).sortBy(_._1).take(10))

    })
    val data:ArrayBuffer[(Double,String)]=new ArrayBuffer[(Double, String)]()
    manualout.map(x=>x._2.map(y=> data+=y))
      val res=data.map(x=> {
        val tmp = data.map(x => ((x._2, x._1), 1L)).groupBy(x => x._1).map(l => (l._1, l._2.map(_._2).reduce(_ + _)))
        tmp
      })


  }
 }
