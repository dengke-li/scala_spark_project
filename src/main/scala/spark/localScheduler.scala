package spark
import scala.collection.mutable.ArrayBuffer
class localScheduler {
  def runJob(rdd:RDD, func:Seq[Int]=>Seq[Int], partitions:Array[Int]):Seq[Seq[Int]]={
    val splits = rdd.splits
    var result = new Array[Seq[Int]](splits.length)
    
    for(partition<-partitions){
      result(partition) = rdd.compute(splits(partition))
    }
    result.toSeq
  }
}