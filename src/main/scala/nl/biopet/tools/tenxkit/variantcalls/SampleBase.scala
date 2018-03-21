package nl.biopet.tools.tenxkit.variantcalls

case class SampleBase(sample: String, contig: String, pos: Long, allele: String, count: Long = 1L) {

}

object SampleBase {
  def from(read: SampleRead): List[SampleBase] = {

    val bases = read.sequence.zip(read.quality).iterator


    Nil
  }
}