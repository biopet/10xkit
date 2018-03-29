package nl.biopet.tools.tenxkit.variantcalls

import java.io.File

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.samtools.{
  SAMRecord,
  SAMSequenceDictionary,
  SamReader,
  SamReaderFactory
}
import nl.biopet.utils.ngs.intervals.BedRecord
import nl.biopet.utils.ngs
import nl.biopet.utils.ngs.fasta.ReferenceRegion

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ReadBam(samReader: SamReader,
              sampleTag: String,
              umiTag: Option[String],
              region: BedRecord,
              dict: SAMSequenceDictionary,
              referenceFile: IndexedFastaSequenceFile,
              correctCells: Map[String, Int],
              minBaseQual: Byte)
    extends Iterator[VariantCall]
    with AutoCloseable {
  private val contig = dict.getSequenceIndex(region.chr)
  private val referenceRegion = ReferenceRegion(
    referenceFile,
    region.chr,
    region.start + 1,
    if (dict.getSequence(contig).getSequenceLength > (region.end + 30))
      region.end + 30
    else dict.getSequence(contig).getSequenceLength
  )
  private val samIt =
    samReader.query(region.chr, region.start - 3, region.end + 3, false)
  private val samItBuffer = samIt.buffered

  def hasNext: Boolean = nextVariantcall.isDefined

  private var position: Int = 0

  private val buffer: mutable.Map[Int, PositionBases] = mutable.Map()

  private var nextVariantcall: Option[VariantCall] = detectNext

  private def fillBuffer(): Unit = {
    while (samItBuffer.hasNext && buffer.isEmpty) {
      fillBuffer(samItBuffer.next())
    }
    if (buffer.nonEmpty) {
      position = buffer.keys.min
      while (samItBuffer.hasNext && samItBuffer.head.getStart < position) {
        fillBuffer(samItBuffer.next())
      }
    }
  }

  private def fillBuffer(read: SAMRecord): Unit = {
    val sample = ReadBam.extractSample(read, correctCells, sampleTag)
    sample match {
      case Some(s) =>
        val umi = ReadBam.extractUmi(read, umiTag)
        val bases = SampleBase
          .createBases(read, contig, s, umi)
          .filter(_._2.avgQual.exists(_ >= minBaseQual))
        bases.foreach {
          case (pos, base) =>
            val position = pos.position.toInt
            if (position > region.start && position <= region.end) {
              if (!buffer.contains(position))
                buffer += position -> PositionBases()
              if (!buffer(position).samples.contains(s))
                buffer(position).samples += s -> mutable.Map()
              val sampleAllele = SampleAllele(base.allele, base.delBases)
              val current = buffer(position)
                .samples(s)
                .getOrElse(sampleAllele, AlleleCount())
              val seen = umi.exists { u =>
                val alreadySeen = buffer(position).umis.contains(u)
                if (!alreadySeen) buffer(position).umis.add(u)
                alreadySeen
              }
              (base.strand, seen) match {
                case (true, true) =>
                  buffer(position).samples(s) += sampleAllele -> current.copy(
                    forwardReads = current.forwardReads + 1)
                case (true, false) =>
                  buffer(position).samples(s) += sampleAllele -> current.copy(
                    forwardReads = current.forwardReads + 1,
                    forwardUmi = current.forwardUmi + 1)
                case (false, true) =>
                  buffer(position).samples(s) += sampleAllele -> current.copy(
                    reverseReads = current.reverseReads + 1)
                case (false, false) =>
                  buffer(position).samples(s) += sampleAllele -> current.copy(
                    reverseReads = current.reverseReads + 1,
                    reverseUmi = current.reverseUmi + 1)
              }
            }
        }
      case _ =>
    }
  }

  private def detectNext: Option[VariantCall] = {
    fillBuffer()
    if (position > region.end || buffer.isEmpty) None
    else {
      val variantCall = Some(
        VariantCall
          .createFromBases(contig, position, buffer(position), referenceRegion))
      buffer -= position
      variantCall
    }
  }

  def next(): VariantCall = {
    nextVariantcall match {
      case Some(n) =>
        nextVariantcall = detectNext
        n
      case _ =>
        throw new IllegalStateException(
          "Iterator is depleted, please check .hasNext")
    }
  }

  def close(): Unit = {
    samIt.close()
  }
}

object ReadBam {
  def extractSample(read: SAMRecord,
                    samples: Map[String, Int],
                    sampleTag: String): Option[Int] = {
    Option(read.getAttribute(sampleTag)).flatMap(s => samples.get(s.toString))
  }

  def extractUmi(read: SAMRecord, umiTag: Option[String]): Option[Int] = {
    umiTag.flatMap(t =>
      Option(read.getAttribute(t)).map(u =>
        ngs.sequenceTo2bitSingleInt(u.toString)))
  }
}
