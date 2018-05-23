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
import htsjdk.variant.variantcontext.{
  Allele,
  GenotypeBuilder,
  VariantContext,
  VariantContextBuilder
}
import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.tools.tenxkit.variantcalls.{
  AlleleCount,
  PositionBases,
  SampleAllele
}
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
                       altAlleles: IndexedSeq[String],
                       samples: Map[Int, IndexedSeq[AlleleCount]]) {

  /** true if there is coverage on a alt allele */
  def hasNonReference: Boolean = altDepth > 0

  /** total umi coverage on this position */
  def totalDepth: Int = samples.values.flatMap(_.map(_.total)).sum

  /** total read coverage on this position */
  def totalReadDepth: Int = samples.values.flatMap(_.map(_.totalReads)).sum

  /** total umi coverage on the reference allele */
  def referenceDepth: Int =
    samples.values.flatMap(_.headOption.map(_.total)).sum

  /** Total umi coverage on the alt alleles */
  def altDepth: Int = totalDepth - referenceDepth

  /** Ratio of  the alt depth to the reference allele */
  def totalAltRatio: Double = altDepth.toDouble / totalDepth

  /** true if there is a sample alt allele with atleast the goiven number of umis */
  def minSampleAltDepth(cutoff: Int): Boolean = {
    samples.values.exists(_.tail.exists(_.total >= cutoff))
  }

  /** Allele alles in 1 Array */
  def allAlleles: Array[String] = Array(refAllele) ++ altAlleles

  /** Umi depth for each allele acrosss samples */
  def alleleDepth: Seq[Int] = {
    allAlleles.indices.map(i =>
      samples.values.flatMap(_.lift(i).map(_.total)).sum)
  }

  /** Read depth for each allele acrosss samples */
  def alleleReadDepth: Seq[Int] = {
    allAlleles.indices.map(i =>
      samples.values.flatMap(_.lift(i).map(_.totalReads)).sum)
  }

  /** This sets the depth of alleles back to 0 if they are below the cutoff */
  def setAllelesToZeroDepth(minAlleleDepth: Int): VariantCall = {
    val newSamples = samples.map {
      case (s, a) =>
        s -> a.map(a => if (a.total >= minAlleleDepth) a else AlleleCount())
    }
    this.copy(samples = newSamples)
  }

  /** This will remove all alleles that does not have any coverage anymore
    * When no alleles are left a None is returned */
  def cleanupAlleles(): Option[VariantCall] = {
    val ad = alleleDepth
    val alleles = allAlleles
    val newAltAlleles =
      alleles.zipWithIndex.tail.filter { case (_, i) => ad(i) > 0 }.map {
        case (allele, _) => allele
      }
    val newSamples = samples
      .map {
        case (sample, a) =>
          sample -> a.zipWithIndex
            .filter { case (_, i) => i == 0 || ad(i) > 0 }
            .map { case (allele, _) => allele }
      }
      .filter { case (idx, list) => list.exists(_.total > 0) }

    if (newSamples.nonEmpty)
      Some(this.copy(altAlleles = newAltAlleles, samples = newSamples))
    else None
  }

  /** This will calculate a pvalue with a binominal test. All above the cutoff will be set to 0 depth */
  def setAllelesToZeroPvalue(seqError: Float,
                             cutoffPvalue: Float): VariantCall = {
    val pvalues = createBinomialPvalues(seqError)
    val newSamples = samples.map {
      case (s, a) =>
        s -> a.zipWithIndex.map {
          case (al, i) =>
            if (pvalues(s)(i) <= cutoffPvalue) al else AlleleCount()
        }
    }
    this.copy(samples = newSamples)
  }

  /** This will calculate a pvalue with a binominal test for each allele and each sample */
  def createBinomialPvalues(seqError: Float): Map[Int, IndexedSeq[Double]] = {
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
          .AD(a.map(_.total).toArray)
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

  def toGroupCall(groupsMap: Map[Int, String]): GroupCall =
    GroupCall.fromVariantCall(this, groupsMap)

  def getUniqueAlleles(groupsMap: Map[Int, String],
                       balance: Double = 0.9): Array[(String, String)] = {
    val groupFactions = samples
      .filter { case (groupId, _) => groupsMap.contains(groupId) }
      .groupBy { case (groupId, _) => groupsMap(groupId) }
      .map {
        case (groupName, cells) =>
          groupName -> allAlleles.indices.map { i =>
            cells.values.count(_(i).total > 1).toDouble / cells.size
          }.toArray
      }
    allAlleles.zipWithIndex.flatMap {
      case (allele, i) =>
        val a = groupFactions.values.count(_(i) >= balance)
        val b = groupFactions.values.count(_(i) <= (1.0 - balance))
        if (a == 1 && b >= 1) {
          groupFactions.find { case (_, f) => f(i) >= balance }.map {
            case (al, _) => al -> allele
          }
        } else None
    }
  }
}

object VariantCall {

  def fromVariantContext(variant: VariantContext,
                         dict: SAMSequenceDictionary,
                         sampleMap: Map[String, Int]): VariantCall = {
    val contig = dict.getSequenceIndex(variant.getContig)
    val pos = variant.getStart
    val refAllele = variant.getReference.getBaseString
    val altAlleles =
      variant.getAlternateAlleles.map(_.getBaseString).toIndexedSeq
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
          }.toIndexedSeq
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
    val maxDel = bases.samples.values.flatMap(_.keys.map(_.delBases)).max
    val end = position + maxDel
    val refAllele = new String(
      referenceRegion.sequence.slice(
        position - referenceRegion.start,
        position - referenceRegion.start + maxDel + 1)).toUpperCase
    val oldAlleles = bases.samples.values.flatMap(_.keySet)
    val newAllelesMap = oldAlleles.map { a =>
      SampleAllele(a.allele, a.delBases) -> (if (a.delBases > 0 || !refAllele
                                                   .startsWith(a.allele)) {
                                               if (a.allele.length == refAllele.length || a.delBases > 0)
                                                 a.allele
                                               else
                                                 new String(
                                                   refAllele.zipWithIndex.map {
                                                     case (nuc, idx) =>
                                                       a.allele
                                                         .lift(idx)
                                                         .getOrElse(nuc)
                                                   }.toArray)
                                             } else refAllele)
    }.toMap
    val altAlleles =
      newAllelesMap.values.filter(_ != refAllele).toArray.distinct
    val allAlleles = IndexedSeq(refAllele) ++ altAlleles

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
    if (inputFile.isDirectory) {
      fromPartitionedVcf(inputFile, reference, sampleMap)
    } else {
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
  }

  def fromPartitionedVcf(directory: File,
                         reference: File,
                         sampleMap: Broadcast[Map[String, Int]])(
      implicit sc: SparkContext): RDD[VariantCall] = {
    val dict = sc.broadcast(fasta.getCachedDict(reference))
    require(directory.isDirectory, s"'$directory' is not a directory")
    val files = directory
      .listFiles()
      .filter(_.getName.endsWith(".vcf.gz"))
      .map(_.getAbsoluteFile)
    sc.parallelize(files, files.size)
      .mapPartitions { it =>
        it.flatMap { file =>
          new VCFFileReader(file)
            .iterator()
            .map(fromVariantContext(_, dict.value, sampleMap.value))
        }
      }
  }
}
