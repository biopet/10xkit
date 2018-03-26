package nl.biopet.tools.tenxkit.variantcalls

case class SampleBase(allele: String,
                      strand: Boolean,
                      qual: List[Byte],
                      delBases: Int = 0) {

  def avgQual: Option[Byte] =
    if (qual.nonEmpty) Some((qual.map(_.toInt).sum / qual.size).toByte) else None
}
