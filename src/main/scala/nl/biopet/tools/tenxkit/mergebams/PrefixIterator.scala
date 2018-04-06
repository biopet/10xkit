package nl.biopet.tools.tenxkit.mergebams

import htsjdk.samtools.{SAMRecord, SAMSequenceDictionary, SamReader}

import scala.collection.JavaConversions._

class PrefixIterator(bamReaders: Map[String, SamReader],
                     dict: SAMSequenceDictionary,
                     sampleTag: String)
    extends Iterator[SAMRecord] {
  private val its = bamReaders.map {
    case (sample, reader) => sample -> reader.iterator().buffered
  }
  def hasNext: Boolean = its.values.exists(_.hasNext)

  def next(): SAMRecord = {
    val sample = its
      .filter(_._2.hasNext)
      .map(x => x._1 -> x._2.head)
      .toList
      .minBy(x =>
        (dict.getSequenceIndex(x._2.getContig), x._2.getAlignmentStart))
      ._1
    val record = its(sample).next()
    Option(record.getAttribute(sampleTag)).foreach(x =>
      record.setAttribute(sampleTag, sample + "-" + x.toString))
    record
  }
}
