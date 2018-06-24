package spark
import scala.collection.mutable.ArrayBuffer
class localScheduler {
  def runJob(rdd:RDD, func:Seq[Int]=>Seq[Int], partitions:Array[Int]):Seq[Iterator[Int]]={
    val splits = rdd.splits
    var result = new Array[Iterator[Int]](splits.length)
    
    for(partition<-partitions){
      result(partition) = rdd.compute(splits(partition))
    }
    result.toSeq
  }
}