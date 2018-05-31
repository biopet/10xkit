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

import htsjdk.samtools.SamReaderFactory
import htsjdk.samtools.reference.IndexedFastaSequenceFile
import nl.biopet.test.BiopetTest
import nl.biopet.tools.tenxkit.VariantCall
import nl.biopet.utils.ngs.intervals.BedRecord
import nl.biopet.utils.ngs.fasta.getCachedDict
import org.testng.annotations.Test

class ReadBamTest extends BiopetTest {
  @Test
  def testNonUmi(): Unit = {
    val samReader =
      SamReaderFactory.makeDefault.open(resourceFile("/read_bam.bam"))
    val dict = getCachedDict(resourceFile("/reference.fasta"))
    val fastaReader = new IndexedFastaSequenceFile(
      resourceFile("/reference.fasta"))

    val readBam = new ReadBam(samReader,
                              "SM",
                              None,
                              BedRecord("chr1", 3, 30),
                              dict,
                              fastaReader,
                              Map("sample1" -> 0, "sample2" -> 1),
                              '0'.toByte,
                              1)
    val result = readBam.toList
    readBam.close()

    result.headOption shouldBe Some(
      VariantCall(0,
                  4,
                  "A",
                  IndexedSeq("T"),
                  Map(0 -> IndexedSeq(AlleleCount(), AlleleCount(3)),
                      1 -> IndexedSeq(AlleleCount(), AlleleCount(3)))))
  }

  @Test
  def testUmi(): Unit = {
    val samReader =
      SamReaderFactory.makeDefault.open(resourceFile("/read_bam.bam"))
    val dict = getCachedDict(resourceFile("/reference.fasta"))
    val fastaReader = new IndexedFastaSequenceFile(
      resourceFile("/reference.fasta"))

    val readBam = new ReadBam(samReader,
                              "SM",
                              Some("UB"),
                              BedRecord("chr1", 3, 30),
                              dict,
                              fastaReader,
                              Map("sample1" -> 0, "sample2" -> 1),
                              '0'.toByte,
                              1)
    val result = readBam.toList
    readBam.close()

    result.headOption shouldBe Some(
      VariantCall(0,
                  4,
                  "A",
                  IndexedSeq("T"),
                  Map(0 -> IndexedSeq(AlleleCount(), AlleleCount(1, 0, 3, 0)),
                      1 -> IndexedSeq(AlleleCount(), AlleleCount(1, 0, 3, 0)))))
  }

  @Test
  def testEmpty(): Unit = {
    val samReader =
      SamReaderFactory.makeDefault.open(resourceFile("/read_bam.bam"))
    val dict = getCachedDict(resourceFile("/reference.fasta"))
    val fastaReader = new IndexedFastaSequenceFile(
      resourceFile("/reference.fasta"))

    val readBam = new ReadBam(samReader,
                              "SM",
                              Some("UB"),
                              BedRecord("chr1", 0, 30),
                              dict,
                              fastaReader,
                              Map("sample1" -> 0),
                              '0'.toByte,
                              1)
    val result = readBam.toList
    intercept[IllegalStateException] {
      readBam.next()
    }.getMessage shouldBe "Iterator is depleted, please check .hasNext"
    readBam.close()
  }
}
