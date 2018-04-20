/*
 * Copyright (c) 2018 Biopet
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package nl.biopet.tools.tenxkit.mergebams

import htsjdk.samtools.{SAMRecord, SAMSequenceDictionary, SamReader}

import scala.collection.JavaConversions._

class PrefixIterator(bamReaders: Map[String, SamReader],
                     dict: SAMSequenceDictionary,
                     sampleTag: String)
    extends Iterator[PrefixIterator.Record] {
  private val its = bamReaders.map {
    case (sample, reader) => sample -> reader.iterator().buffered
  }
  def hasNext: Boolean = its.values.exists(_.hasNext)

  def next(): PrefixIterator.Record = {
    val nextRecords = its
      .filter { case (_, x) => x.hasNext }
      .map { case (key, it) => key -> it.head }
      .toList
    val (sample, _) = nextRecords.minBy {
      case (_, r) =>
        val contig = dict.getSequenceIndex(r.getContig)
        (if (contig >= 0) contig else Int.MaxValue, r.getAlignmentStart)
    }
    val record = its(sample).next()
    val newBarcode = Option(record.getAttribute(sampleTag))
      .map(x => sample + "-" + x.toString)
    newBarcode.foreach(record.setAttribute(sampleTag, _))
    PrefixIterator.Record(newBarcode, record)
  }
}

object PrefixIterator {
  case class Record(newBarcode: Option[String], record: SAMRecord)
}
