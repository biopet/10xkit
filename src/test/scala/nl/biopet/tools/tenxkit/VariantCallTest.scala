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

import htsjdk.variant.variantcontext.{
  Allele,
  GenotypeBuilder,
  VariantContextBuilder
}
import nl.biopet.test.BiopetTest
import nl.biopet.tools.tenxkit.variantcalls.{
  AlleleCount,
  PositionBases,
  SampleAllele
}
import org.testng.annotations.Test
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.ngs.fasta.ReferenceRegion
import nl.biopet.utils.spark
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import scala.collection.mutable

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
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(1)),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(1)))
    )

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
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(1)),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(1)))
    )

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
  def testCreateBinomialPvalues(): Unit = {
    val v1 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(1)),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(1)))
    )
    v1.createBinomialPvalues(0.005f) shouldBe Map(
      0 -> IndexedSeq(7.474999666401416E-5,
                      7.474999666401416E-5,
                      7.474999666401416E-5),
      1 -> IndexedSeq(7.474999666401416E-5,
                      7.474999666401416E-5,
                      7.474999666401416E-5)
    )

    val v2 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(), AlleleCount()),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount(1)))
    )
    v2.createBinomialPvalues(0.005f) shouldBe Map(
      0 -> IndexedSeq(1.0, 1.0, 1.0),
      1 -> IndexedSeq(7.474999666401416E-5,
                      7.474999666401416E-5,
                      7.474999666401416E-5)
    )

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
      0 -> IndexedSeq(AlleleCount(2), AlleleCount(2), AlleleCount()),
      1 -> IndexedSeq(AlleleCount(2), AlleleCount(2), AlleleCount()))
  }

  @Test
  def testCleanupAlleles(): Unit = {
    val v1 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount()),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount()))
    )
    v1.cleanupAlleles().map(_.altAlleles) shouldBe Some(IndexedSeq("G"))

    val v2 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(), AlleleCount(), AlleleCount()),
          1 -> IndexedSeq(AlleleCount(), AlleleCount(), AlleleCount()))
    )
    v2.cleanupAlleles() shouldBe None
  }

  @Test
  def testSetAllelesToZeroPvalue(): Unit = {
    val v1 = VariantCall(
      1,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1000), AlleleCount(1000), AlleleCount(1)),
          1 -> IndexedSeq(AlleleCount(1000), AlleleCount(1000), AlleleCount(1)))
    )
    v1.setAllelesToZeroPvalue(0.005f, 0.05f)
      .samples
      .values
      .flatten
      .toList shouldBe Map(0 -> IndexedSeq(AlleleCount(1000),
                                           AlleleCount(1000),
                                           AlleleCount()),
                           1 -> IndexedSeq(
                             AlleleCount(1000),
                             AlleleCount(1000),
                             AlleleCount())).values.flatten.toList
  }

  @Test
  def testVariantContext(): Unit = {
    val dict = fasta.getCachedDict(resourceFile("/reference.fasta"))
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount()),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount()))
    )
    val c1 = v1.toVariantContext(IndexedSeq("sample1", "sample2"), dict, 0.005f)
    val v2 = VariantCall.fromVariantContext(c1,
                                            dict,
                                            Map("sample1" -> 0, "sample2" -> 1))
    v2 shouldBe v1
  }

  @Test
  def testMissingFields(): Unit = {
    val dict = fasta.getCachedDict(resourceFile("/reference.fasta"))

    val base = new VariantContextBuilder()
      .chr("chr1")
      .start(2)
      .alleles("G", "C")
      .computeEndFromAlleles(List(Allele.create("G", true), Allele.create("C")),
                             2)

    val c1 = base
      .genotypes(
        new GenotypeBuilder()
          .name("sample1")
          .attribute("ADR", Array(5, 5).mkString(","))
          .attribute("ADF", Array(5, 5).mkString(","))
          .attribute("ADR-READ", Array(5, 5).mkString(","))
          .attribute("ADF-READ", Array(5, 5).mkString(","))
          .make())
      .make()
    val v1 = VariantCall.fromVariantContext(c1, dict, Map("sample1" -> 0))
    v1.contig shouldBe 0
    v1.samples.keySet shouldBe Set(0)

    val c2 = base.genotypes(new GenotypeBuilder().name("sample1").make()).make()
    val v2 = VariantCall.fromVariantContext(c2, dict, Map("sample1" -> 0))
    v2.contig shouldBe 0
    v2.samples.keySet shouldBe Set()
  }

  @Test
  def testVcfFile(): Unit = {
    implicit val sc: SparkContext =
      spark.loadSparkContext("test", Some("local[1]"))
    val cells = sc.broadcast(IndexedSeq("sample1", "sample2"))
    val cellsMap = sc.broadcast(Map("sample1" -> 0, "sample2" -> 1))
    val dict =
      sc.broadcast(fasta.getCachedDict(resourceFile("/reference.fasta")))
    val header = sc.broadcast(vcfHeader(cells.value))
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount()),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount()))
    )
    val v2 = VariantCall(
      0,
      2000,
      "A",
      IndexedSeq("G", "T"),
      Map(0 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount()),
          1 -> IndexedSeq(AlleleCount(1), AlleleCount(1), AlleleCount()))
    )
    val outputDir = File.createTempFile("test.", ".vcf")
    outputDir.delete()
    outputDir.mkdir()
    val rdd = sc.parallelize(List(v1, v2), 2)
    VariantCall.writeToPartitionedVcf(rdd,
                                      outputDir,
                                      cells,
                                      dict,
                                      header,
                                      0.005f)
    outputDir.list().count(_.endsWith(".vcf.gz")) shouldBe 2
    outputDir.list().count(_.endsWith(".vcf.gz.tbi")) shouldBe 2

    val singleFile1 = VariantCall
      .fromVcfFile(new File(outputDir, "0.vcf.gz"),
                   dict,
                   cellsMap,
                   Int.MaxValue)
      .collect()
    val singleFile2 = VariantCall
      .fromVcfFile(new File(outputDir, "1.vcf.gz"),
                   dict,
                   cellsMap,
                   Int.MaxValue)
      .collect()
    singleFile1(0) shouldBe v1
    singleFile2(0) shouldBe v2

    val totalRead1 = VariantCall
      .fromVcfFile(outputDir, dict, cellsMap, Int.MaxValue)
      .collect()
      .toList
    val totalRead2 =
      VariantCall.fromPartitionedVcf(outputDir, dict, cellsMap).collect().toList
    totalRead1 shouldBe totalRead2
    totalRead1 shouldBe List(v1, v2)

    sc.stop()
  }

  @Test
  def testCreateFromBases(): Unit = {
    val bases1 = PositionBases(
      samples =
        mutable.Map(0 -> mutable.Map(SampleAllele("C", 0) -> AlleleCount(5))))
    val region =
      ReferenceRegion(resourceFile("/reference.fasta"), "chr1", 1, 2000)
    VariantCall.createFromBases(0, 1000, PositionBases(), region, 2) shouldBe None
    val v1 = VariantCall.createFromBases(0, 1000, bases1, region, 2)
    v1 shouldBe Some(
      VariantCall(0,
                  1000,
                  "A",
                  IndexedSeq("C"),
                  Map(0 -> IndexedSeq(AlleleCount(), AlleleCount(5)))))

    val bases2 = PositionBases(
      samples =
        mutable.Map(0 -> mutable.Map(SampleAllele("A", 2) -> AlleleCount(5))))
    val v2 = VariantCall.createFromBases(0, 1000, bases2, region, 2)
    v2 shouldBe Some(
      VariantCall(0,
                  1000,
                  "ACA",
                  IndexedSeq("A"),
                  Map(0 -> IndexedSeq(AlleleCount(), AlleleCount(5)))))

    val bases3 = PositionBases(
      samples = mutable.Map(
        0 -> mutable.Map(SampleAllele("A", 2) -> AlleleCount(5)),
        1 -> mutable.Map(SampleAllele("A", 0) -> AlleleCount(5)),
        2 -> mutable.Map(SampleAllele("AG", 0) -> AlleleCount(5))
      ))
    val v3 = VariantCall.createFromBases(0, 1000, bases3, region, 2)
    v3 shouldBe Some(
      VariantCall(
        0,
        1000,
        "ACA",
        IndexedSeq("AGA", "A"),
        Map(
          2 -> IndexedSeq(AlleleCount(), AlleleCount(5), AlleleCount()),
          1 -> IndexedSeq(AlleleCount(5), AlleleCount(), AlleleCount()),
          0 -> IndexedSeq(AlleleCount(), AlleleCount(), AlleleCount(5))
        )
      ))

  }
}
