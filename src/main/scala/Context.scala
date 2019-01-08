import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types._
import scala.collection.mutable

trait Context {
  val conf = new SparkConf().setMaster("local[4]").setAppName("lsh")
  val spark = SparkSession.builder.config(conf).getOrCreate()

}
