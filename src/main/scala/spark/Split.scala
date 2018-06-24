package spark
class Split(seq:Seq[Int]) extends Serializable{
  /**
   * Get the split's index within its parent RDD
   */
  val index: Int=0
  
  // A better default implementation of HashCode
  override def hashCode(): Int = index
  def getSeq():Seq[Int] = seq
}