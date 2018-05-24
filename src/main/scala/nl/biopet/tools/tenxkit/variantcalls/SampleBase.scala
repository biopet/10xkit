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

package nl.biopet.tools.tenxkit.variantcalls

import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord, TextCigarCodec}

import scala.collection.mutable
import scala.collection.JavaConversions._

case class SampleBase(allele: String,
                      strand: Boolean,
                      qual: List[Byte],
                      delBases: Int = 0) {

  def avgQual: Option[Byte] =
    if (qual.nonEmpty) Some((qual.map(_.toInt).sum / qual.size).toByte)
    else None
}

object SampleBase {

  def createBases(read: SAMRecord): List[(Int, SampleBase)] = {
    if (read.getReadUnmappedFlag) Nil
    else
      SampleBase.createBases(read.getAlignmentStart,
                             !read.getReadNegativeStrandFlag,
                             read.getReadBases,
                             read.getBaseQualityString.getBytes,
                             read.getCigar)
  }

  def createBases(start: Int,
                  strand: Boolean,
                  sequence: Array[Byte],
                  quality: Array[Byte],
                  cigar: String): List[(Int, SampleBase)] = {
    createBases(start, strand, sequence, quality, TextCigarCodec.decode(cigar))
  }

  def createBases(start: Int,
                  strand: Boolean,
                  sequence: Array[Byte],
                  quality: Array[Byte],
                  cigar: Cigar): List[(Int, SampleBase)] = {
    val seqIt = sequence.zip(quality).toList.toIterator

    val referenceBuffer = mutable.Map[Int, SampleBase]()
    var refPos = start
    for (element <- cigar) {
      element.getOperator match {
        case CigarOperator.SOFT_CLIP | CigarOperator.S =>
          seqIt.drop(element.getLength)
        case CigarOperator.MATCH_OR_MISMATCH | CigarOperator.EQ |
            CigarOperator.X | CigarOperator.M =>
          seqIt.take(element.getLength).foreach {
            case (base, qual) =>
              referenceBuffer += refPos -> SampleBase(base.toChar.toString,
                                                      strand,
                                                      qual :: Nil)
              refPos += 1
          }
        case CigarOperator.INSERTION | CigarOperator.I =>
          val seq = seqIt.take(element.getLength).toList
          referenceBuffer.get(refPos - 1) match {
            case Some(b) =>
              referenceBuffer += (refPos - 1) -> b.copy(
                allele = b.allele ++ seq.map { case (x, _) => x.toChar },
                qual = b.qual ++ seq.map { case (_, x)     => x.toByte })
            case _ =>
              throw new IllegalStateException(
                "Insertion without a base found, cigar start with I (or after the S/H)")
          }
        case CigarOperator.DELETION | CigarOperator.D =>
          referenceBuffer.get(refPos - 1) match {
            case Some(b) =>
              referenceBuffer += (refPos - 1) -> b.copy(
                delBases = b.delBases + element.getLength)
            case _ =>
              throw new IllegalStateException(
                "Deletion without a base found, cigar start with D (or after the S/H)")
          }
          (refPos to (element.getLength + refPos)).foreach(p =>
            referenceBuffer += p -> SampleBase("", strand, Nil))
          refPos += element.getLength
        case CigarOperator.SKIPPED_REGION | CigarOperator.N =>
          refPos += element.getLength
        case CigarOperator.HARD_CLIP | CigarOperator.H | CigarOperator.PADDING |
            CigarOperator.P =>
      }
    }
    require(!seqIt.hasNext, "After cigar parsing sequence is not depleted")
    referenceBuffer.toList
  }
}
