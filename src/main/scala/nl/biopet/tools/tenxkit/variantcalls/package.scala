package nl.biopet.tools.tenxkit

import scala.collection.mutable

package object variantcalls {
  case class Position(contig: Int, position: Long)

  case class Cutoffs(minAlternativeDepth: Int = 2,
                     minCellAlternativeDepth: Int = 2,
                     minTotalDepth: Int = 5,
                     minBaseQual: Byte = '*'.toByte,
                     maxPvalue: Float = 0.05f)

  case class PositionBases(
                            samples: mutable.Map[Int, mutable.Map[SampleAllele, AlleleCount]] =
                            mutable.Map(),
                            umis: mutable.Set[Int] = mutable.Set())

  case class SampleAllele(allele: String, delBases: Int)

}
