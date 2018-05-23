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

package nl.biopet.tools.tenxkit

import java.io.File

import nl.biopet.test.BiopetTest
import nl.biopet.tools.tenxkit.variantcalls.AlleleCount
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.spark
import org.apache.spark.SparkContext
import org.testng.annotations.Test

class GroupCallTest extends BiopetTest {
  @Test
  def testBasicAlleleMethods(): Unit = {
    val groupsMap = Map(0 -> "g2", 1 -> "g1", 2 -> "g2")
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G"),
      Map(0 -> IndexedSeq(AlleleCount(10), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(5), AlleleCount(5)),
          2 -> IndexedSeq(AlleleCount(0), AlleleCount(10)))
    )
    val g1 = v1.toGroupCall(groupsMap)

    g1.allAlleles shouldBe Array("A", "G")
    g1.alleleDepth shouldBe Seq(15, 15)
    g1.alleleReadDepth shouldBe Seq(15, 15)
  }

  @Test
  def testBasicTotalMethods(): Unit = {
    val groupsMap = Map(0 -> "g2", 1 -> "g1", 2 -> "g2")
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G"),
      Map(0 -> IndexedSeq(AlleleCount(10), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(5), AlleleCount(5)),
          2 -> IndexedSeq(AlleleCount(0), AlleleCount(10)))
    )
    val g1 = v1.toGroupCall(groupsMap)

    g1.altDepth shouldBe 15
    g1.referenceDepth shouldBe 15
    g1.totalDepth shouldBe 30
    g1.totalReadDepth shouldBe 30
    g1.hasNonReference shouldBe true
    g1.totalAltRatio shouldBe 0.5
  }
  @Test
  def testCreate(): Unit = {
    val groupsMap = Map(0 -> "g2", 1 -> "g1", 2 -> "g2")
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G"),
      Map(0 -> IndexedSeq(AlleleCount(10), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(5), AlleleCount(5)),
          2 -> IndexedSeq(AlleleCount(0), AlleleCount(10)))
    )
    val g1 = v1.toGroupCall(groupsMap)
    g1 shouldBe GroupCall(
      v1.contig,
      v1.pos,
      v1.refAllele,
      v1.altAlleles,
      Map("g1" -> IndexedSeq(AlleleCount(5, 0, 5, 0), AlleleCount(5, 0, 5, 0)),
          "g2" -> IndexedSeq(AlleleCount(10, 0, 10, 0),
                             AlleleCount(10, 0, 10, 0))),
      Map("g1" -> GenotypeCall.fromAd(IndexedSeq(5, 5)),
          "g2" -> GenotypeCall.fromAd(IndexedSeq(10, 10))),
      Map("g1" -> 1, "g2" -> 2)
    )
  }

  @Test
  def testVcfFile(): Unit = {
    val groupsMap = Map(0 -> "g2", 1 -> "g1", 2 -> "g2")
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G"),
      Map(0 -> IndexedSeq(AlleleCount(10), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(5), AlleleCount(5)),
          2 -> IndexedSeq(AlleleCount(0), AlleleCount(10)))
    )
    val g1 = v1.toGroupCall(groupsMap)
    val g2 = g1.copy(pos = 2000)
    val outputDir = File.createTempFile("test.", ".vcf")
    outputDir.delete()
    outputDir.mkdir()

    implicit val sc: SparkContext =
      spark.loadSparkContext("test", Some("local[1]"))
    val dict =
      sc.broadcast(fasta.getCachedDict(resourceFile("/reference.fasta")))
    val header = sc.broadcast(vcfHeader(IndexedSeq("g1", "g2")))

    val rdd = sc.parallelize(List(g1, g2))

    GroupCall.writeAsPartitionedVcfFile(rdd, outputDir, header, dict).collect()
    val fromVcf = GroupCall.fromPartitionedVcf(outputDir, dict).collect().toList
    fromVcf shouldBe List(g1, g2)
    sc.stop()
  }
}
