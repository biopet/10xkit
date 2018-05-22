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

import htsjdk.samtools.SAMSequenceDictionary
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder
import htsjdk.variant.variantcontext.{
  Allele,
  GenotypeBuilder,
  VariantContext,
  VariantContextBuilder
}
import htsjdk.variant.vcf.VCFHeader
import nl.biopet.tools.tenxkit.variantcalls.AlleleCount
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

case class GroupCall(contig: Int,
                     pos: Long,
                     refAllele: String,
                     altAlleles: Array[String],
                     alleleCount: Map[String, Array[AlleleCount]],
                     genotypes: Map[String, GenotypeCall],
                     cellCounts: Map[String, Int]) {

  def hasNonReference: Boolean = {
    altDepth > 0
  }

  def totalDepth: Int = {
    alleleCount.values.flatMap(_.map(_.total)).sum
  }

  def totalReadDepth: Int = {
    alleleCount.values.flatMap(_.map(_.totalReads)).sum
  }

  def referenceDepth: Int = {
    alleleCount.values.flatMap(_.headOption.map(_.total)).sum
  }

  def altDepth: Int = {
    totalDepth - referenceDepth
  }

  def totalAltRatio: Double = {
    altDepth.toDouble / totalDepth
  }

  def allAlleles: Array[String] = Array(refAllele) ++ altAlleles

  def alleleDepth: Seq[Int] = {
    allAlleles.indices.map(i =>
      alleleCount.values.flatMap(_.lift(i).map(_.total)).sum)
  }

  def alleleReadDepth: Seq[Int] = {
    allAlleles.indices.map(i =>
      alleleCount.values.flatMap(_.lift(i).map(_.totalReads)).sum)
  }

  def toVariantContext(dict: SAMSequenceDictionary): VariantContext = {
    val alleles = Allele.create(refAllele, true) :: altAlleles
      .map(Allele.create)
      .toList

    val genotypes = alleleCount.map {
      case (groupName, a) =>
        val genotypeCall = this.genotypes(groupName)
        val attributes: Map[String, String] = Map(
          "CN" -> cellCounts(groupName).toString,
          "DP-READ" -> a.map(_.totalReads).sum.toString,
          "DPF" -> a.map(_.forwardUmi).sum.toString,
          "DPR" -> a.map(_.reverseUmi).sum.toString,
          "AD-READ" -> a.map(_.totalReads).mkString(","),
          "ADF-READ" -> a.map(_.forwardReads).mkString(","),
          "ADR-READ" -> a.map(_.reverseReads).mkString(","),
          "ADF" -> a.map(_.forwardUmi).mkString(","),
          "ADR" -> a.map(_.reverseUmi).mkString(",")
        )
        new GenotypeBuilder(groupName)
          .alleles(genotypeCall.genotype.flatten.map(a => alleles(a)).toList)
          .AD(a.map(_.total))
          .DP(a.map(_.total).sum)
          .attributes(attributes)
          .make()
    }
    val attributes: Map[String, Any] =
      Map(
        "DP" -> totalDepth,
        "DP-READ" -> totalReadDepth,
        "SN" -> alleleCount.size,
        "CN" -> cellCounts.values.sum,
        "AD" -> alleleDepth.mkString(","),
        "AD-READ" -> alleleReadDepth.mkString(",")
      )
    new VariantContextBuilder()
      .chr(dict.getSequence(contig).getSequenceName)
      .start(pos)
      .attributes(attributes)
      .computeEndFromAlleles(alleles, pos.toInt)
      .alleles(alleles)
      .genotypes(genotypes)
      .make()
  }
}

object GroupCall {
  def fromVariantCall(variant: VariantCall,
                      groupsMap: Map[Int, String]): GroupCall = {
    val alleleCounts =
      variant.samples
        .groupBy { case (k, _) => groupsMap.get(k) }
        .flatMap { case (k, v) => k.map(_ -> v) }
        .map {
          case (group, maps) =>
            group -> maps.values
              .flatMap(x => x.zipWithIndex)
              .groupBy { case (_, idx) => idx }
              .map {
                case (_, counts) =>
                  counts.map { case (c, _) => c }.reduce(_ + _)
              }
              .toArray
        }
    val cellCounts =
      variant.samples
        .groupBy { case (k, _) => groupsMap.get(k) }
        .flatMap { case (k, v) => k.map(_ -> v) }
        .map {
          case (k, v) => k -> v.size
        }
    val genotypes = alleleCounts.map {
      case (k, v) => k -> GenotypeCall.fromAd(v.map(_.total))
    }
    GroupCall(variant.contig,
              variant.pos,
              variant.refAllele,
              variant.altAlleles,
              alleleCounts,
              genotypes,
              cellCounts)
  }

  def writeAsPartitionedVcfFile(rdd: RDD[GroupCall],
                                outputDir: File,
                                vcfHeader: Broadcast[VCFHeader],
                                dict: Broadcast[SAMSequenceDictionary])(
      implicit sc: SparkContext): RDD[File] = {
    outputDir.mkdir()
    rdd
      .mapPartitionsWithIndex {
        case (idx, it) =>
          val outputFile = new File(outputDir, s"$idx.vcf.gz")
          val writer =
            new VariantContextWriterBuilder()
              .setOutputFile(outputFile)
              .build()
          writer.writeHeader(vcfHeader.value)
          it.foreach(x => writer.add(x.toVariantContext(dict.value)))
          writer.close()
          Iterator(outputFile)
      }
  }
}
