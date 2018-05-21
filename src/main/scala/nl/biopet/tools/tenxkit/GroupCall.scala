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

import htsjdk.samtools.SAMSequenceDictionary
import htsjdk.variant.variantcontext.{
  Allele,
  GenotypeBuilder,
  VariantContext,
  VariantContextBuilder
}
import nl.biopet.tools.tenxkit.variantcalls.AlleleCount

import scala.collection.JavaConversions._

case class GroupCall(contig: Int,
                     pos: Long,
                     refAllele: String,
                     altAlleles: Array[String],
                     alleleCount: Map[String, Array[AlleleCount]],
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
        val genotypeCall = GenotypeCall.fromAd(a.map(_.total))
        val attributes = Map(
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
    val attributes =
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
      variant.samples.groupBy { case (k, _) => groupsMap(k) }.map {
        case (group, maps) =>
          group -> maps.values
            .flatMap(x => x.zipWithIndex)
            .groupBy { case (_, idx) => idx }
            .map(_._2.map(_._1).reduce(_ + _))
            .toArray
      }
    val cellCounts =
      variant.samples.groupBy { case (k, _) => groupsMap(k) }.map {
        case (k, v) => k -> v.size
      }
    GroupCall(variant.contig,
              variant.pos,
              variant.refAllele,
              variant.altAlleles,
              alleleCounts,
              cellCounts)
  }
}
