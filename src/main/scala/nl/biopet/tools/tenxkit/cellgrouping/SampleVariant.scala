package nl.biopet.tools.tenxkit.cellgrouping

import nl.biopet.tools.tenxkit.variantcalls.AlleleCount

case class SampleVariant(sample: Int, contig: Int, pos: Long, refDepth: Int, altDepth: Array[Int]) {

}
