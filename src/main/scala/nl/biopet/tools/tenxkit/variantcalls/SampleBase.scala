package nl.biopet.tools.tenxkit.variantcalls

case class SampleBase(sample: String,
                      contig: String,
                      pos: Long,
                      allele: String,
                      strand: Boolean,
                      qual: List[Byte],
                      delBases: Int = 0) {

  def avgQual: Option[Byte] =
    if (qual.nonEmpty) Some((qual.sum / qual.size).toByte) else None
}
