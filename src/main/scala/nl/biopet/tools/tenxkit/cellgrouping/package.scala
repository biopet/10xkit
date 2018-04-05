package nl.biopet.tools.tenxkit

package object cellgrouping {
  case class SampleCombinationKey(sample1: Int, sample2: Int)
  case class SampleCombinationValue(ad1: Array[Int], ad2: Array[Int])
}
