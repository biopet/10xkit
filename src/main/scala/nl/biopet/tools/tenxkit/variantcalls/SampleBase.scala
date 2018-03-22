package nl.biopet.tools.tenxkit.variantcalls

case class SampleBase(sample: String,
                      contig: String,
                      pos: Long,
                      allele: String,
                      strand: Boolean,
                      qual: List[Char],
                      delBases: Int = 0) {

  def avgQual: Option[Char] = if (qual.nonEmpty) Some((qual.sum / qual.size).toChar) else None
}
