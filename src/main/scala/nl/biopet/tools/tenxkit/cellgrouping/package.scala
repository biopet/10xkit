package nl.biopet.tools.tenxkit

package object cellgrouping {
  case class SampleCombinationKey(sample1: Int, sample2: Int)
  case class AlleleDepth(ad1: Array[Int], ad2: Array[Int])
  case class FractionPair(f1: Double, f2: Double) {
    def midlePoint: Double = ((f1-f2)/2)+f1
    def distance: Double = math.sqrt(math.pow(midlePoint - f1, 2) * 2)
  }
}
