package nl.biopet.tools.tenxkit

package object variantcalls {
  case class Position(contig: Int, position: Long)

  case class Cutoffs(                minAlternativeDepth: Int = 2,
                                     minCellAlternativeDepth: Int = 2,
                                     minTotalDepth: Int = 5,
                                     minBaseQual: Byte = '*'.toByte
                    )
}
