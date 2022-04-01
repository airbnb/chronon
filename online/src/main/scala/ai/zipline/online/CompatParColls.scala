package ai.zipline.online

object CompatParColls {
  val Converters = {
    import Compat._

    {
      import scala.collection.parallel._

      CollectionConverters
    }
  }

  object Compat {
    object CollectionConverters
  }
}
