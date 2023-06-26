package ai.chronon.spark

import org.apache.spark.sql.{
  Column,
  Dataset,
  Encoder,
  KeyValueGroupedDataset,
}

trait SparkUtils {

  def cogroupSorted[K, V, U, R: Encoder](
                                          leftDataset: KeyValueGroupedDataset[K, V],
                                          rightDataset: KeyValueGroupedDataset[K, U],
                                          leftOrdering: Seq[Column],
                                          rightOrdering: Seq[Column],
                                          f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]
                                        ): Dataset[R]

}
