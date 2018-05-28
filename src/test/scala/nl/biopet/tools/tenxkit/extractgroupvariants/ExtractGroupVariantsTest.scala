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

package nl.biopet.tools.tenxkit.extractgroupvariants

import java.io.File

import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.{GroupCall, VariantCall}
import nl.biopet.tools.tenxkit.variantcalls.AlleleCount
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.spark
import nl.biopet.utils.test.tools.ToolTest
import org.apache.spark.SparkContext
import org.testng.annotations.Test

class ExtractGroupVariantsTest extends ToolTest[Args] {
  def toolCommand: ExtractGroupVariants.type = ExtractGroupVariants
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      ExtractGroupVariants.main(Array())
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
  def testDefault(): Unit = {
    val outputDir = File.createTempFile("extractgroups.", ".out")
    outputDir.delete()
    outputDir.mkdir()
    ExtractGroupVariants.main(
      Array(
        "-R",
        resourcePath("/reference.fasta"),
        "-i",
        testVcfFile.getAbsolutePath,
        "--sparkMaster",
        "local[1]",
        "--correctCells",
        resourcePath("/samples.txt"),
        "-o",
        outputDir.getAbsolutePath,
        "-g",
        s"group1=${resourcePath("/group1.txt")}",
        "-g",
        s"group2=${resourcePath("/group2.txt")}"
      ))

    implicit val sc: SparkContext =
      spark.loadSparkContext("test", Some("local[1]"))
    val dict =
      sc.broadcast(fasta.getCachedDict(resourceFile("/reference.fasta")))

    val groupsCalls = GroupCall
      .fromPartitionedVcf(new File(outputDir, "output-vcf"), dict)
      .collect()
    groupsCalls.length shouldBe 1
    groupsCalls.headOption.map(_.genotypes.keySet) shouldBe Some(
      Set("group1", "group2"))
    groupsCalls.headOption.map(_.contig) shouldBe Some(0)
    groupsCalls.headOption.map(_.pos) shouldBe Some(1000)
    groupsCalls.headOption.map(_.refAllele) shouldBe Some("A")
    groupsCalls.headOption.map(_.altAlleles) shouldBe Some(IndexedSeq("G"))
    groupsCalls.headOption.map(_.alleleCount("group1")) shouldBe Some(
      IndexedSeq(AlleleCount(20), AlleleCount()))
    groupsCalls.headOption.map(_.alleleCount("group2")) shouldBe Some(
      IndexedSeq(AlleleCount(), AlleleCount(10)))

    sc.stop()
  }
}
