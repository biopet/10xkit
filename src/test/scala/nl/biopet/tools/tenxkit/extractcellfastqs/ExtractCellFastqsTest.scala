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

package nl.biopet.tools.tenxkit.extractcellfastqs

import java.io.File

import htsjdk.samtools.fastq.FastqReader
import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.Test

import scala.collection.JavaConversions._

class ExtractCellFastqsTest extends ToolTest[Args] {
  def toolCommand: ExtractCellFastqs.type = ExtractCellFastqs
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      ExtractCellFastqs.main(Array())
    }
  }

  @Test
  def testDefault(): Unit = {
    val outputDir = File.createTempFile("test.", ".out")
    outputDir.delete()
    outputDir.mkdirs()
    outputDir.deleteOnExit()
    val sampleTag = "RG"
    ExtractCellFastqs.main(
      Array(
        "-i",
        resourcePath("/wgs2.realign.bam"),
        "-R",
        resourcePath("/reference.fasta"),
        "--sparkMaster",
        "local[2]",
        "--sampleTag",
        sampleTag,
        "-o",
        outputDir.getAbsolutePath,
        "--correctCells",
        resourcePath("/wgs2.readgroups.txt")
      ))

    readsFastq(new File(outputDir, "wgs2-lib1_R1.fq.gz")) + readsFastq(
      new File(outputDir, "wgs2-lib2_R1.fq.gz")) shouldBe 10000 - 8
    readsFastq(new File(outputDir, "wgs2-lib1_R2.fq.gz")) + readsFastq(
      new File(outputDir, "wgs2-lib2_R2.fq.gz")) shouldBe 10000 - 8
  }

  def readsFastq(file: File): Long = {
    val reader = new FastqReader(file)
    try {
      reader.iterator().length
    } finally {
      reader.close()
    }
  }
}
