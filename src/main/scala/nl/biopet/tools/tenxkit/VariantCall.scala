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

import cern.jet.random.Binomial
import cern.jet.random.engine.RandomEngine
import htsjdk.samtools.SAMSequenceDictionary
import htsjdk.variant.variantcontext.{Allele, GenotypeBuilder, VariantContext, VariantContextBuilder}
import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.tools.tenxkit.variantcalls.{AlleleCount, PositionBases, SampleAllele}
import nl.biopet.utils.ngs.{fasta, vcf}
import nl.biopet.utils.ngs.fasta.ReferenceRegion
import nl.biopet.utils.ngs.intervals.BedRecordList
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

case class VariantCall(contig: Int,
                       pos: Long,
                       refAllele: String,
                       altAlleles: Array[String],
                       samples: Map[Int, Array[AlleleCount]]) {
  def hasNonReference: Boolean = {
    altDepth > 0
  }

  def totalDepth: Int = {
    samples.map(_._2.map(_.total).sum).sum
  }

  def totalReadDepth: Int = {
    samples.map(_._2.map(_.totalReads).sum).sum
  }

  def referenceDepth: Int = {
    samples.map(_._2.headOption.map(_.total).getOrElse(0)).sum
  }

  def altDepth: Int = {
    totalDepth - referenceDepth
  }

  def totalAltRatio: Double = {
    altDepth.toDouble / totalDepth
  }

  def minSampleAltDepth(cutoff: Int): Boolean = {
    samples.values.exists(_.tail.exists(_.total >= cutoff))
  }

  def toVcfLine(samplesIdxs: Range, dict: SAMSequenceDictionary): String = {
    s"${dict.getSequence(contig).getSequenceName}\t$pos\t.\t$refAllele\t${altAlleles
      .mkString(",")}\t.\t.\t.\tAD\t" +
      s"${samplesIdxs
        .map { idx =>
          samples.get(idx).map(a => s"${a.map(_.total).mkString(",")}").getOrElse(".")
        }
        .mkString("\t")}"
  }

  def setAllelesToZeroDepth(minAlleleDepth: Int): VariantCall = {
    val newSamples = samples.map {
      case (s, a) =>
        s -> a.map(a => if (a.totalReads > minAlleleDepth) a else AlleleCount())
    }
    this.copy(samples = newSamples)
  }

  def cleanupAlleles(): Option[VariantCall] = {
    val ad = alleleDepth
    val alleles = allAlleles
    val newAltAlleles =
      alleles.zipWithIndex.tail.filter(i => ad(i._2) > 0).map(_._1)
    val newSamples = samples
      .map {
        case (sample, a) =>
          sample -> a.zipWithIndex
            .filter(i => i._2 == 0 || ad(i._2) > 0)
            .map(_._1)
      }
      .filter(_._2.exists(_.total > 0))

    if (newSamples.nonEmpty)
      Some(this.copy(altAlleles = newAltAlleles, samples = newSamples))
    else None
  }

  def setAllelesToZeroPvalue(seqError: Float,
                             cutoffPvalue: Float): VariantCall = {
    val pvalues = createBinomialPvalues(seqError)
    val newSamples = samples.map {
      case (s, a) =>
        s -> a.zipWithIndex.map(a =>
          if (pvalues(s)(a._2) <= cutoffPvalue) a._1 else AlleleCount())
    }
    this.copy(samples = newSamples)
  }

  def createBinomialPvalues(seqError: Float): Map[Int, Array[Double]] = {
    samples.map {
      case (s, a) =>
        val totalReads = a.map(_.totalReads).sum
        if (totalReads > 1) {
          val binomial =
            new Binomial(totalReads, seqError, RandomEngine.makeDefault())
          s -> a.map(b => 1.0 - binomial.cdf(b.totalReads))
        } else s -> a.map(b => 1.0)
    }
  }

  def allAlleles: Array[String] = Array(refAllele) ++ altAlleles

  def alleleDepth: Seq[Int] =
    allAlleles.indices.map(i => samples.map(_._2(i).total).sum)
  def alleleReadDepth: Seq[Int] =
    allAlleles.indices.map(i => samples.map(_._2(i).totalReads).sum)

  def toVariantContext(sampleList: Array[String],
                       dict: SAMSequenceDictionary,
                       seqError: Float): VariantContext = {
    val seqErrors = createBinomialPvalues(seqError)

    val genotypes = samples.map {
      case (sample, a) =>
        val attributes = Map(
          "SEQ-ERR" -> seqErrors(sample).map(_.toString).mkString(","),
          "DP-READ" -> a.map(_.totalReads).sum.toString,
          "DPF" -> a.map(_.forwardUmi).sum.toString,
          "DPR" -> a.map(_.reverseUmi).sum.toString,
          "AD-READ" -> a.map(_.totalReads).mkString(","),
          "ADF-READ" -> a.map(_.forwardReads).mkString(","),
          "ADR-READ" -> a.map(_.reverseReads).mkString(","),
          "ADF" -> a.map(_.forwardUmi).mkString(","),
          "ADR" -> a.map(_.reverseUmi).mkString(",")
        )
        new GenotypeBuilder(sampleList(sample))
          .AD(a.map(_.total))
          .DP(a.map(_.total).sum)
          .attributes(attributes)
          .make()
    }
    val alleles = Allele.create(refAllele, true) :: altAlleles
      .map(Allele.create)
      .toList
    val attributes =
      Map("DP" -> totalDepth,
          "DP-READ" -> totalReadDepth,
          "SN" -> samples.size,
          "AD" -> alleleDepth.mkString(","),
          "AD-READ" -> alleleReadDepth.mkString(","))
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

object VariantCall {

  def fromVariantContext(variant: VariantContext,
                         dict: SAMSequenceDictionary,
                         sampleMap: Map[String, Int]): VariantCall = {
    val contig = dict.getSequenceIndex(variant.getContig)
    val pos = variant.getStart
    val refAllele = variant.getReference.getBaseString
    val altAlleles = variant.getAlternateAlleles.map(_.getBaseString).toArray
    val alleleIndencies = (Array(refAllele) ++ altAlleles).zipWithIndex
    val genotypes = variant.getGenotypes.flatMap { g =>
      (Option(g.getExtendedAttribute("ADR"))
         .map(_.toString.split(",").map(_.toInt)),
       Option(g.getExtendedAttribute("ADF"))
         .map(_.toString.split(",").map(_.toInt)),
       Option(g.getExtendedAttribute("ADR-READ"))
         .map(_.toString.split(",").map(_.toInt)),
       Option(g.getExtendedAttribute("ADF-READ"))
         .map(_.toString.split(",").map(_.toInt))) match {
        case (Some(adr), Some(adf), Some(adrRead), Some(adfRead)) =>
          val alleles = alleleIndencies.map {
            case (_, i) => AlleleCount(adf(i), adr(i), adfRead(i), adrRead(i))
          }
          Some(sampleMap(g.getSampleName) -> alleles)
        case _ => None
      }
    }
    VariantCall(contig, pos, refAllele, altAlleles, genotypes.toMap)
  }

  def createFromBases(contig: Int,
                      position: Int,
                      bases: PositionBases,
                      referenceRegion: ReferenceRegion,
                      minCellAlleleCoverage: Int): Option[VariantCall] = {
    val maxDel = bases.samples.flatMap(_._2.map(_._1.delBases)).max
    val end = position + maxDel
    val refAllele = new String(
      referenceRegion.sequence.slice(
        position - referenceRegion.start,
        position - referenceRegion.start + maxDel + 1)).toUpperCase
    val oldAlleles = bases.samples.flatMap(_._2.keySet)
    val newAllelesMap = oldAlleles.map { a =>
      SampleAllele(a.allele, a.delBases) -> (if (a.delBases > 0 || !refAllele
                                                   .startsWith(a.allele)) {
                                               if (a.allele.length == refAllele.length || a.delBases > 0)
                                                 a.allele
                                               else
                                                 new String(
                                                   refAllele.zipWithIndex
                                                     .map(x =>
                                                       a.allele
                                                         .lift(x._2)
                                                         .getOrElse(x._1))
                                                     .toArray)
                                             } else refAllele)
    }.toMap
    val altAlleles =
      newAllelesMap.values.filter(_ != refAllele).toArray.distinct
    val allAlleles = Array(refAllele) ++ altAlleles

    if (altAlleles.isEmpty) None
    else {
      val samples = bases.samples
        .map {
          case (sample, sampleBases) =>
            val alleles =
              sampleBases.map {
                case (allele, alleleBases) =>
                  newAllelesMap(allele) -> alleleBases
              }
            sample -> allAlleles.map(x => alleles.getOrElse(x, AlleleCount()))
        }
        .filter {
          case (sample, alleles) =>
            alleles.exists(_.total >= minCellAlleleCoverage)
        }
      Some(VariantCall(contig, position, refAllele, altAlleles, samples.toMap))
    }
  }

  def fromVcfFile(inputFile: File,
                  reference: File,
                  sampleMap: Broadcast[Map[String, Int]],
                  binsize: Int)(implicit sc: SparkContext): RDD[VariantCall] = {
    val dict = sc.broadcast(fasta.getCachedDict(reference))
    val regions =
      BedRecordList.fromReference(reference).scatter(binsize)
    sc.parallelize(regions, regions.size).mapPartitions { it =>
      it.flatMap { list =>
        vcf
          .loadRegions(inputFile, list.iterator)
          .map(VariantCall.fromVariantContext(_, dict.value, sampleMap.value))
      }
    }
  }

  def fromPartitionedVcf(directory: File,
                         reference: File,
                         sampleMap: Broadcast[Map[String, Int]])(implicit sc: SparkContext): RDD[VariantCall] = {
    val dict = sc.broadcast(fasta.getCachedDict(reference))
    require(directory.isDirectory, s"'$directory' is not a directory")
    val files = directory.listFiles().filter(_.getName.endsWith(".vcf.gz")).map(_.getAbsoluteFile)
    sc.parallelize(files)
      .mapPartitions { it =>
      it.flatMap { file =>
        new VCFFileReader(file).iterator().map(fromVariantContext(_, dict.value, sampleMap.value))
      }
    }
  }
}
