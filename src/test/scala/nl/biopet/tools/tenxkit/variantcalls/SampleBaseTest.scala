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

import htsjdk.samtools.{SAMFileHeader, SAMRecord}
import nl.biopet.test.BiopetTest
import nl.biopet.utils.ngs.fasta.getCachedDict
import org.testng.annotations.Test

class SampleBaseTest extends BiopetTest {
  @Test
  def testCreate(): Unit = {
    val seq = "AATTCCGGAA"
    val bases = SampleBase.createBases(1,
                                       true,
                                       seq.getBytes,
                                       "AAAAAAAAAA".getBytes,
                                       "3M1D3M1I3M")
    val sortedBases = bases.sortBy { case (x, _) => x }
    sortedBases.size shouldBe seq.length
    sortedBases.map { case (_, x) => x.allele }.mkString shouldBe "AATTCCGGAA"
    val (_, base2) = sortedBases(2)
    val (_, base6) = sortedBases(6)
    base2.delBases shouldBe 1
    base6.allele shouldBe "CG"
  }

  @Test
  def testSamRecord(): Unit = {
    val dict = getCachedDict(resourceFile("/reference.fasta"))
    val samHeader = new SAMFileHeader()
    val read = new SAMRecord(samHeader)
    val seq = "AATTCCGGAA"
    read.setAlignmentStart(1)
    read.setReferenceName("chr1")
    read.setCigarString("3M1D3M1I3M")
    read.setReadBases(seq.getBytes)
    read.setBaseQualities("AAAAAAAAAA".getBytes)
    val bases = SampleBase.createBases(read)
    val sortedBases = bases.sortBy { case (x, _) => x }
    sortedBases.size shouldBe seq.length
    sortedBases.map { case (_, x) => x.allele }.mkString shouldBe "AATTCCGGAA"
    val (_, base2) = sortedBases(2)
    val (_, base6) = sortedBases(6)
    base2.delBases shouldBe 1
    base6.allele shouldBe "CG"
  }

  @Test
  def testUnMapped(): Unit = {
    val dict = getCachedDict(resourceFile("/reference.fasta"))
    val samHeader = new SAMFileHeader()
    val read = new SAMRecord(samHeader)
    val seq = "AATTCCGGAA"
    read.setReadUnmappedFlag(true)
    SampleBase.createBases(read) shouldBe Nil
  }

  @Test
  def testWrongCigars(): Unit = {
    val seq = "AATTCCGGAA"
    def bases(cigar: String) =
      SampleBase.createBases(1,
                             true,
                             seq.getBytes,
                             "AAAAAAAAAA".getBytes,
                             cigar)

    intercept[IllegalStateException] {
      bases("3I1D3M1I3M")
    }.getMessage shouldBe "Insertion without a base found, cigar start with I (or after the S/H)"

    intercept[IllegalStateException] {
      bases("3D3M1D3M1I3M")
    }.getMessage shouldBe "Deletion without a base found, cigar start with D (or after the S/H)"

    intercept[IllegalStateException] {
      bases("3M1D3M1I2M")
    }.getMessage shouldBe "Sequence is to long for cigar string"

    intercept[IllegalStateException] {
      bases("3M1D3M1I4M")
    }.getMessage shouldBe "Sequence is to short for cigar string"
  }
}
