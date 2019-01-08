import org.apache.spark.ml.linalg.Vectors
import LshEuclidean._
object Driver extends App with Context {

  LSHmodel.usingRdd("/home/rochan/Sparkout/frame1","/home/rochan/Sparkout/frame2")
  Verify.checkSimilarity("/home/rochan/Sparkout/frame1")


}
