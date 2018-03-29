package nl.biopet.tools.tenxkit.variantcalls

import java.io.File

import htsjdk.samtools.{SAMRecord, SAMSequenceDictionary, SamReader, SamReaderFactory}
import nl.biopet.utils.ngs.intervals.BedRecord
import nl.biopet.utils.ngs

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ReadBam(samReader: SamReader,
              sampleTag: String,
              umiTag: Option[String],
              region: BedRecord,
              dict: SAMSequenceDictionary,
              referenceFile: File,
              correctCells: Map[String, Int]) extends Iterator[VariantCall] with AutoCloseable {
  private val contig = dict.getSequenceIndex(region.chr)
  private val samIt = samReader.query(region.chr, region.start - 3, region.end + 3, false)
  private val samItBuffer = samIt.buffered
  private var nextVariantcall: Option[VariantCall] = detectNext

  def hasNext: Boolean = nextVariantcall.isDefined

  private var position: Int = 0

  private val buffer: mutable.Map[Int, ListBuffer[SampleBase]] = mutable.Map()

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
        val bases = SampleBase.createBases(read, contig, s, umi)
        bases.foreach { case (pos, base) =>
          val position = pos.position.toInt
          if (!buffer.contains(pos.position)) buffer += position -> new ListBuffer()
          buffer(position).add(base)
        }
      case _ =>
    }
  }

  private def detectNext: Option[VariantCall] = {
    fillBuffer()
    if (position > region.end || buffer.isEmpty) None
    else {


      ???
    }
  }

  def next(): VariantCall = {
    nextVariantcall match {
      case Some(n) =>
        nextVariantcall = detectNext
        n
      case _ => throw new IllegalStateException("Iterator is depleted, please check .hasNext")
    }
  }

  def close(): Unit = {
    samIt.close()
  }
}

object ReadBam {
  def extractSample(read: SAMRecord, samples: Map[String, Int], sampleTag: String): Option[Int] = {
    Option(read.getAttribute(sampleTag)).flatMap(samples.get)
  }

  def extractUmi(read: SAMRecord, umiTag: Option[String]): Option[Int] = {
    umiTag.flatMap(t => Option(read.getAttribute(t)).map(u => ngs.sequenceTo2bitSingleInt(u.toString)))
  }

}
