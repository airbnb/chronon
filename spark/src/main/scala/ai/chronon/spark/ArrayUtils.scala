package ai.chronon.spark
import scala.collection.JavaConverters._

object ArrayUtils {

  /**
    * Converts a Java ArrayList to a Scala Seq
    * Adding this so that we can easily convert Java ArrayLists to Scala Seqs in PySpark.
    * @param javaList a Java ArrayList
    * @tparam T the type of the elements in the ArrayList
    * @return a Scala Seq
    */
  def JavaArrayListToScalaSeq[T](javaList: java.util.ArrayList[T]): Seq[T] = {
    javaList.asScala.toSeq
  }

}
