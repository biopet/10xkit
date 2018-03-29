package nl.biopet.tools.tenxkit.variantcalls

import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord, TextCigarCodec}

import scala.collection.mutable
import scala.collection.JavaConversions._

case class SampleBase(sample: Int,
                      allele: String,
                      strand: Boolean,
                      qual: List[Byte],
                      delBases: Int = 0,
                      umi: Option[Int] = None) {

  def avgQual: Option[Byte] =
    if (qual.nonEmpty) Some((qual.map(_.toInt).sum / qual.size).toByte)
    else None
}

object SampleBase {

  def createBases(read: SAMRecord,
                  contig: Int,
                  sample: Int,
                  umi: Option[Int]): List[(Position, SampleBase)] = {
    if (read.getReadUnmappedFlag) Nil
    else
      SampleBase.createBases(contig,
                             read.getAlignmentStart,
                             sample,
                             !read.getReadNegativeStrandFlag,
                             read.getReadBases,
                             read.getBaseQualityString.getBytes,
                             read.getCigar,
                             umi)
  }

  def createBases(contig: Int,
                  start: Long,
                  sample: Int,
                  strand: Boolean,
                  sequence: Array[Byte],
                  quality: Array[Byte],
                  cigar: String,
                  umni: Option[Int]): List[(Position, SampleBase)] = {
    createBases(contig,
                start,
                sample,
                strand,
                sequence,
                quality,
                TextCigarCodec.decode(cigar),
                umni)
  }

  def createBases(contig: Int,
                  start: Long,
                  sample: Int,
                  strand: Boolean,
                  sequence: Array[Byte],
                  quality: Array[Byte],
                  cigar: Cigar,
                  umni: Option[Int]): List[(Position, SampleBase)] = {
    val seqIt = sequence.zip(quality).toList.toIterator

    val referenceBuffer = mutable.Map[Long, SampleBase]()
    var refPos = start
    for (element <- cigar) {
      element.getOperator match {
        case CigarOperator.SOFT_CLIP =>
          seqIt.drop(element.getLength)
        case CigarOperator.MATCH_OR_MISMATCH | CigarOperator.EQ |
            CigarOperator.X =>
          seqIt.take(element.getLength).foreach {
            case (base, qual) =>
              referenceBuffer += refPos -> SampleBase(sample,
                                                      base.toChar.toString,
                                                      strand,
                                                      qual :: Nil,
                                                      umi = umni)
              refPos += 1
          }
        case CigarOperator.INSERTION =>
          val seq = seqIt.take(element.getLength).toList
          referenceBuffer.get(refPos - 1) match {
            case Some(b) =>
              referenceBuffer += (refPos - 1) -> b.copy(
                allele = b.allele ++ seq.map(_._1.toChar),
                qual = b.qual ++ seq.map(_._2.toByte))
            case _ =>
              throw new IllegalStateException(
                "Insertion without a base found, cigar start with I (or after the S/H)")
          }
        case CigarOperator.DELETION =>
          referenceBuffer.get(refPos - 1) match {
            case Some(b) =>
              referenceBuffer += (refPos - 1) -> b.copy(
                delBases = b.delBases + element.getLength)
            case _ =>
              throw new IllegalStateException(
                "Deletion without a base found, cigar start with D (or after the S/H)")
          }
          (refPos to (element.getLength + refPos)).foreach(
            p =>
              referenceBuffer += p -> SampleBase(sample,
                                                 "",
                                                 strand,
                                                 Nil,
                                                 umi = umni))
          refPos += element.getLength
        case CigarOperator.SKIPPED_REGION                    => refPos += element.getLength
        case CigarOperator.HARD_CLIP | CigarOperator.PADDING =>
      }
    }
    require(!seqIt.hasNext, "After cigar parsing sequence is not depleted")
    referenceBuffer.map(x => Position(contig, x._1) -> x._2).toList
  }
}
