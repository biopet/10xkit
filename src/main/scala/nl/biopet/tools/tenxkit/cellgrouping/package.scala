package nl.biopet.tools.tenxkit

package object cellgrouping {
  case class SampleCombination(contig: Int, pos: Int, sample1: Int, sample2: Int)
  case class SampleCombinationKey(sample1: Int, sample2: Int)
  case class SampleCombinationValue() //TODO: add values
}
