package ai.chronon.spark.stats

import com.yahoo.sketches.kll.KllFloatsSketch

object KllSketchDistance {

  /** L-infinity distance for two sketches based on equally spaced bins */
  def numericalDistance(sketch1: KllFloatsSketch, sketch2: KllFloatsSketch, bins: Int = 128): Double = {
    val keySet = sketch1.getQuantiles(bins).union(sketch2.getQuantiles(bins))
    var linfSimple = 0.0
    keySet.foreach { key =>
      val cdf1 = sketch1.getRank(key)
      val cdf2 = sketch2.getRank(key)
      val cdfDiff = Math.abs(cdf1 - cdf2)
      linfSimple = Math.max(linfSimple, cdfDiff)
    }
    linfSimple
  }

}
