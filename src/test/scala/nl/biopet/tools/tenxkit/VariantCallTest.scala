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

import nl.biopet.test.BiopetTest
import nl.biopet.tools.tenxkit.variantcalls.AlleleCount
import org.testng.annotations.Test
import nl.biopet.utils.ngs.fasta

class VariantCallTest extends BiopetTest {
  @Test
  def testUniqueAlleles(): Unit = {
    val call1 = VariantCall(
      1,
      1,
      "A",
      IndexedSeq("C"),
      Map(1 -> IndexedSeq(AlleleCount(2), AlleleCount(2)),
          2 -> IndexedSeq(AlleleCount(2), AlleleCount(2))))
    call1.getUniqueAlleles(Map(1 -> "group1", 2 -> "group2")) shouldBe empty

    val call2 = VariantCall(1,
                            1,
                            "A",
                            IndexedSeq("C"),
                            Map(1 -> IndexedSeq(AlleleCount(2), AlleleCount(2)),
                                2 -> IndexedSeq(AlleleCount(), AlleleCount(2))))
    call2.getUniqueAlleles(Map(1 -> "group1", 2 -> "group2")) shouldBe Array(
      "group1" -> "A")
  }

  @Test
  def testBasicAlleleMethods(): Unit = {
    val v1 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> Array(AlleleCount(1), AlleleCount(1), AlleleCount(1)),
          1 -> Array(AlleleCount(1), AlleleCount(1), AlleleCount(1))))

    v1.allAlleles shouldBe Array("A", "G", "T")
    v1.alleleDepth shouldBe Seq(2, 2, 2)
    v1.alleleReadDepth shouldBe Seq(2, 2, 2)
  }

  @Test
  def testBasicTotalMethods(): Unit = {
    val v1 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> Array(AlleleCount(1), AlleleCount(1), AlleleCount(1)),
          1 -> Array(AlleleCount(1), AlleleCount(1), AlleleCount(1))))

    v1.altDepth shouldBe 4
    v1.referenceDepth shouldBe 2
    v1.totalDepth shouldBe 6
    v1.totalReadDepth shouldBe 6
    v1.hasNonReference shouldBe true
    v1.totalAltRatio shouldBe 2.0 / 3

    v1.minSampleAltDepth(1) shouldBe true
    v1.minSampleAltDepth(2) shouldBe false
  }

  @Test
  def testSetAllelesToZeroDepth(): Unit = {
    val v1 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(2), AlleleCount(2), AlleleCount(1)),
          1 -> IndexedSeq(AlleleCount(2), AlleleCount(2), AlleleCount(1)))
    )
    v1.setAllelesToZeroDepth(2).samples shouldBe Map(
      0 -> IndexedSeq(AlleleCount(2), AlleleCount(2), AlleleCount(0)),
      1 -> IndexedSeq(AlleleCount(2), AlleleCount(2), AlleleCount(0)))
  }

  @Test
  def testCleanupAlleles(): Unit = {
    val v1 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(0)))
    )
    v1.cleanupAlleles().get.altAlleles shouldBe Array("G")

    val v2 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(0), AlleleCount(0), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(0), AlleleCount(0), AlleleCount(0)))
    )
    v2.cleanupAlleles() shouldBe None
  }

  @Test
  def testSetAllelesToZeroPvalue(): Unit = {
    val v1 = VariantCall(
      1,
      1000,
      "A",
      Array("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1000), AlleleCount(1000), AlleleCount(1)),
          1 -> IndexedSeq(AlleleCount(1000), AlleleCount(1000), AlleleCount(1)))
    )
    v1.setAllelesToZeroPvalue(0.005f, 0.05f)
      .samples
      .values
      .flatten
      .toList shouldBe Map(0 -> IndexedSeq(AlleleCount(1000),
                                           AlleleCount(1000),
                                           AlleleCount(0)),
                           1 -> IndexedSeq(
                             AlleleCount(1000),
                             AlleleCount(1000),
                             AlleleCount(0))).values.flatten.toList
  }

  @Test
  def testVariantContext(): Unit = {
    val dict = fasta.getCachedDict(resourceFile("/reference.fasta"))
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(0)))
    )
    val c1 = v1.toVariantContext(Array("sample1", "sample2"), dict, 0.005f)
    val v2 = VariantCall.fromVariantContext(c1,
                                            dict,
                                            Map("sample1" -> 0, "sample2" -> 1))
    v2 shouldBe v1
  }
}
