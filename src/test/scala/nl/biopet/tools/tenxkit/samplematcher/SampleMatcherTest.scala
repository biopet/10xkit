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

package nl.biopet.tools.tenxkit.samplematcher

import java.io.File

import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.Test

class SampleMatcherTest extends ToolTest[Args] {
  def toolCommand: SampleMatcher.type = SampleMatcher
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      SampleMatcher.main(Array())
    }
  }

  @Test
  def testDefault(): Unit = {
    val outputDir = File.createTempFile("samplematcher.", ".out")
    outputDir.delete()
    outputDir.mkdir()
    SampleMatcher.main(
      Array(
        "--sparkMaster",
        "local[1]",
        "-i",
        resourcePath("/wgs2.realign.bam"),
        "-R",
        resourcePath("/reference.fasta"),
        "-o",
        outputDir.getAbsolutePath,
        "-c",
        "2",
        "--correctCells",
        resourcePath("/wgs2.readgroups.txt"),
        "--sampleTag",
        "RG"
      ))

    val variantcallingDir = new File(outputDir, "variantcalling")
    val calulateDistanceDir = new File(outputDir, "calculatedistance")
    val groupDistanceDir = new File(outputDir, "groupdistance")

    variantcallingDir should exist
    new File(variantcallingDir, "filter-vcf") should exist

    calulateDistanceDir should exist
    new File(calulateDistanceDir, "count.positions.csv") should exist
    new File(calulateDistanceDir, "distance.pow4.csv") should exist
    new File(calulateDistanceDir, "distance.corrected.pow4.csv") should exist

    groupDistanceDir should exist
    new File(groupDistanceDir, "trash.txt") should exist
    groupDistanceDir.list().count(_.endsWith(".txt")) shouldBe 3
  }
}
