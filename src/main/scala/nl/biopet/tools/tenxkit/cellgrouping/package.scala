package nl.biopet.tools.tenxkit

package object cellgrouping {
  case class SampleCombinationKey(sample1: Int, sample2: Int)
  case class AlleleDepth(ad1: Array[Int], ad2: Array[Int])

  case class FractionPairDistance(f1: Double, f2: Double, distance: Double)
  object FractionPairDistance {
    def apply(f1: Double, f2: Double): FractionPairDistance = FractionPairDistance(f1, f2, distanceMidle(f1, f2))
    def midlePoint(f1: Double, f2: Double): Double = ((f1-f2)/2)+f1
    def distanceMidle(f1: Double, f2: Double): Double = math.sqrt(math.pow(midlePoint(f1, f2) - f1, 2) * 2)
  }
}
