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

package nl.biopet.tools.tenxkit.calculatedistance

import java.io.File

import nl.biopet.tools.tenxkit.{DistanceMatrix, VariantCall}
import nl.biopet.tools.tenxkit.variantcalls.AlleleCount
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.spark
import nl.biopet.utils.test.tools.ToolTest
import org.apache.spark.SparkContext
import org.testng.annotations.Test
import nl.biopet.tools.tenxkit

class CalulateDistanceTest extends ToolTest[Args] {
  def toolCommand: CalulateDistance.type = CalulateDistance
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      CalulateDistance.main(Array())
    }
  }

  val testVcfFile: File = {
    val variant = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G"),
      Map(0 -> IndexedSeq(AlleleCount(10), AlleleCount()),
          1 -> IndexedSeq(AlleleCount(10), AlleleCount()),
          2 -> IndexedSeq(AlleleCount(), AlleleCount(10)))
    )

    implicit val sc: SparkContext =
      spark.loadSparkContext("test", Some("local[1]"))
    val cells = sc.broadcast(IndexedSeq("sample1", "sample2", "sample3"))
    val header = sc.broadcast(tenxkit.vcfHeader(cells.value))
    val dict =
      sc.broadcast(fasta.getCachedDict(resourceFile("/reference.fasta")))

    val outputFile = File.createTempFile("test.", "_vcf")
    outputFile.delete()
    outputFile.mkdir()
    VariantCall.writeToPartitionedVcf(sc.parallelize(List(variant)),
                                      outputFile,
                                      cells,
                                      dict,
                                      header,
                                      0.005f)
    sc.stop()
    outputFile
  }

  @Test
  def testDefaultBam(): Unit = {
    val outputDir = File.createTempFile("calculate_distance.", ".out")
    outputDir.delete()
    outputDir.mkdir()
    CalulateDistance.main(
      Array(
        "-i",
        resourcePath("/wgs2.realign.bam"),
        "-o",
        outputDir.getAbsolutePath,
        "--sparkMaster",
        "local[1]",
        "-R",
        resourcePath("/reference.fasta"),
        "--correctCells",
        resourcePath("/wgs2.readgroups.txt"),
        "--sampleTag",
        "RG"
      ))

    new File(outputDir, "distance.pow4.csv") should exist
    new File(outputDir, "count.positions.csv") should exist
    val vcfDir = new File(outputDir, "filter-vcf")
    vcfDir should exist
    for (i <- (0 until 200).par) {
      new File(vcfDir, s"$i.vcf.gz") should exist
      new File(vcfDir, s"$i.vcf.gz.tbi") should exist
    }
  }

  @Test
  def testDefaultVcf(): Unit = {
    val outputDir = File.createTempFile("calculate_distance.", ".out")
    outputDir.delete()
    outputDir.mkdir()
    CalulateDistance.main(
      Array(
        "-i",
        testVcfFile.getAbsolutePath,
        "-o",
        outputDir.getAbsolutePath,
        "--sparkMaster",
        "local[1]",
        "-R",
        resourcePath("/reference.fasta"),
        "--correctCells",
        resourcePath("/samples.txt")
      ))

    val distanceMatrixFile = new File(outputDir, "distance.pow4.csv")
    distanceMatrixFile should exist
    new File(outputDir, "count.positions.csv") should exist
    new File(outputDir, "filter-vcf") shouldNot exist
    val scatterDir = new File(outputDir, "scatters")
    scatterDir shouldNot exist

    val matrix = DistanceMatrix.fromFile(distanceMatrixFile)
    matrix.samples shouldBe IndexedSeq("sample1", "sample2", "sample3")
    matrix.values shouldBe IndexedSeq(
      IndexedSeq(None, Some(0.0), Some(0.5000000000000001)),
      IndexedSeq(None, None, Some(0.5000000000000001)),
      IndexedSeq(None, None, None)
    )
  }

  @Test
  def testScatterVcf(): Unit = {
    val outputDir = File.createTempFile("calculate_distance.", ".out")
    outputDir.delete()
    outputDir.mkdir()
    CalulateDistance.main(
      Array(
        "-i",
        testVcfFile.getAbsolutePath,
        "-o",
        outputDir.getAbsolutePath,
        "--sparkMaster",
        "local[1]",
        "-R",
        resourcePath("/reference.fasta"),
        "--correctCells",
        resourcePath("/samples.txt"),
        "--writeScatters"
      ))

    val distanceMatrixFile = new File(outputDir, "distance.pow4.csv")
    distanceMatrixFile should exist
    new File(outputDir, "count.positions.csv") should exist
    new File(outputDir, "filter-vcf") shouldNot exist
    val scatterDir = new File(outputDir, "scatters")
    scatterDir should exist
  }
}
