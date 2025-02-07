package ai.chronon.flink.window

/**
 * Chronon data from Flink is stored in the KV store as tiles. These tiles are of
 * fixed sizes and contain the intermediate results for a given time range. We keep
 * track of the allowed tile sizes in this file.
 *
 * For more info on tiling, see https://chronon.ai/Tiled_Architecture.html.
 *
 * Note: Because Chronon batch jobs run at 00:00 UTC, tile sizes must
 * be divisible by 24 hours.
 *
 * @param seconds The size of the tile in seconds
 * @param millis The size of the tile in milliseconds
 * @param keyString The key used to store the tile in the KV store
 */
sealed trait TileSize {
  val seconds: Long // The size of the tile in seconds
  val keyString: String // The string representation of the tile size, used as the part of the key stored in the KV store.
  lazy val millis: Long = seconds * 1000L
}

object TileSize {
  // Get TileSize from seconds
  def fromSeconds(seconds: Long): TileSize = seconds match {
    case FiveMinutes.seconds   => FiveMinutes
    case TwentyMinutes.seconds => TwentyMinutes
    case OneHour.seconds       => OneHour
    case SixHours.seconds      => SixHours
    case OneDay.seconds        => OneDay
    case _ =>
      throw new IllegalArgumentException(s"Invalid tile size provided: $seconds seconds")
  }

  // Get TileSize from millis
  def fromMillis(millis: Long): TileSize = fromSeconds(millis / 1000L)

  final case object FiveMinutes extends TileSize {
    val seconds: Long = 300L
    val keyString: String = "5m"
  }

  final case object TwentyMinutes extends TileSize {
    val seconds: Long = 1200L
    val keyString: String = "20m"
  }

  final case object OneHour extends TileSize {
    val seconds: Long = 3600L
    val keyString: String = "1h"
  }

  final case object SixHours extends TileSize {
    val seconds: Long = 21600L
    val keyString: String = "6h"
  }

  final case object OneDay extends TileSize {
    val seconds: Long = 86400L
    val keyString: String = "1d"
  }
}
