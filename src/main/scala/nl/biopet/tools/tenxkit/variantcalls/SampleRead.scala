package nl.biopet.tools.tenxkit.variantcalls

import htsjdk.samtools.{CigarOperator, TextCigarCodec}

import scala.collection.JavaConversions._
import scala.collection.mutable

case class SampleRead(sample: String,
                      contig: String,
                      start: Long,
                      end: Long,
                      sequence: String,
                      quality: String,
                      cigar: String,
                      strand: Boolean) {

  lazy val sampleBases: List[SampleBase] = {
    val seqIt = sequence.zip(quality).iterator

    val referenceBuffer = mutable.Map[Long, SampleBase]()
    var refPos = start
    for (element <- TextCigarCodec.decode(cigar)) {
      element.getOperator match {
        case CigarOperator.SOFT_CLIP =>
          seqIt.drop(element.getLength)
        case CigarOperator.MATCH_OR_MISMATCH | CigarOperator.EQ | CigarOperator.X =>
          seqIt.take(element.getLength).foreach { case (base, qual) =>
            referenceBuffer += refPos -> SampleBase(sample, contig, refPos, base.toString, strand, qual :: Nil)
              refPos += 1
          }
        case CigarOperator.INSERTION =>
          val seq = seqIt.take(element.getLength).toList
          referenceBuffer.get(refPos - 1) match {
            case Some(b) => referenceBuffer += (refPos - 1) -> b.copy(allele = b.allele ++ seq.map(_._1), qual = b.qual ++ seq.map(_._2))
            case _ => throw new IllegalStateException("Insertion without a base found, cigar start with I (or after the S/H)")
          }
        case CigarOperator.DELETION =>
          referenceBuffer.get(refPos - 1) match {
            case Some(b) => referenceBuffer += (refPos - 1) -> b.copy(delBases = b.delBases + element.getLength)
            case _ => throw new IllegalStateException("Deletion without a base found, cigar start with D (or after the S/H)")
          }
          (refPos to (element.getLength + refPos)).foreach(p => referenceBuffer += p -> SampleBase(sample, contig, p, "", strand, Nil))
          refPos += element.getLength
        case CigarOperator.SKIPPED_REGION => refPos += element.getLength
        case CigarOperator.HARD_CLIP | CigarOperator.PADDING =>
      }
    }
    require(!seqIt.hasNext, "After cigar parsing sequence is not depleted")
    referenceBuffer.values.toList.sortBy(_.pos)
  }
}
