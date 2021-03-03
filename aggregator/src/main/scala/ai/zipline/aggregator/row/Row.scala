package ai.zipline.aggregator.row

trait Row {
  def get(index: Int): Any

  def ts: Long

  val length: Int

  def getAs[T](index: Int): T = get(index).asInstanceOf[T]
}
