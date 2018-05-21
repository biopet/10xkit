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

import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder
import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.{TenxKit, VariantCall}
import nl.biopet.utils.io
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ExtractGroupVariants extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    val sparkConf: SparkConf =
      new SparkConf(true).setMaster(cmdArgs.sparkMaster)
    implicit val sparkSession: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    //import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val dict = sc.broadcast(fasta.getCachedDict(cmdArgs.reference))

    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)
    val groups = sc.broadcast(cmdArgs.groups.map {
      case (name, file) =>
        name -> io.getLinesFromFile(file).map(correctCellsMap.value).toArray
    })
    val groupsMap = sc.broadcast(groups.value.flatMap {
      case (k, l) => l.map(_ -> k)
    })
    val vcfHeader = sc.broadcast(tenxkit.vcfHeader(groups.value.keys.toArray))

    val variants = VariantCall
      .fromVcfFile(cmdArgs.inputVcfFile,
                   cmdArgs.reference,
                   correctCellsMap,
                   1000000)
      .sortBy(x => (x.contig, x.pos), ascending = true, numPartitions = 200)
    val groupCalls = variants.map(_.toGroupCall(groupsMap.value))

    val filterGroupCalls =
      groupCalls
        .filter(_.genotypes.size > 1)
        .filter { g =>
          val called =
            g.genotypes.values.filter(_.genotype.exists(_.isDefined))
          called.headOption match {
            case Some(gt) =>
              !called.tail.forall(_.genotype sameElements gt.genotype)
            case _ => false
          }
        }
        .filter { g =>
          val gts = g.genotypes.values
            .filter(_.genotype.exists(_.isDefined))
            .toList
            .distinct
          gts.exists(x =>
            g.genotypes.values.count(_.genotype sameElements x.genotype) == 1)
        }
        .filter(_.alleleCount.values.forall(_.map(_.total).sum >= 50))

    val outputFilterVcfDir = new File(cmdArgs.outputDir, "output-filter-vcf")
    outputFilterVcfDir.mkdir()
    val outputFilterFiles = filterGroupCalls
      .mapPartitionsWithIndex {
        case (idx, it) =>
          val outputFile = new File(outputFilterVcfDir, s"$idx.vcf.gz")
          val writer =
            new VariantContextWriterBuilder()
              .setOutputFile(outputFile)
              .build()
          writer.writeHeader(vcfHeader.value)
          it.foreach(x => writer.add(x.toVariantContext(dict.value)))
          writer.close()
          Iterator(outputFile)
      }
      .collectAsync()

    val outputVcfDir = new File(cmdArgs.outputDir, "output-vcf")
    outputVcfDir.mkdir()
    val outputFiles = variants
      .map(_.toGroupCall(groupsMap.value))
      .mapPartitionsWithIndex {
        case (idx, it) =>
          val outputFile = new File(outputVcfDir, s"$idx.vcf.gz")
          val writer =
            new VariantContextWriterBuilder()
              .setOutputFile(outputFile)
              .build()
          writer.writeHeader(vcfHeader.value)
          it.foreach(x => writer.add(x.toVariantContext(dict.value)))
          writer.close()
          Iterator(outputFile)
      }
      .collectAsync()

    Await.result(outputFilterFiles, Duration.Inf)
    Await.result(outputFiles, Duration.Inf)
  }

  def descriptionText: String =
    """
      |This tool will merge variants from the same group. This will be a representation of the real samples.
      |This can also be used to validate if there is a true set known.
    """.stripMargin

  def manualText: String =
    """
      |This tool will require a vcf file that contains a AD for each cell that has coverage on a position.
      |From here the sample for each group given with the --group argument will compress this group in 1 single vcf genotype.
    """.stripMargin

  def exampleText: String =
    s"""
      |Default run:
      |${TenxKit.sparkExample(
         toolName,
         "--sparkMaster",
         "<spark master>",
         "-i",
         "<input vcf file>",
         "-o",
         "<output directory>",
         "-R",
         "<reference fasta>",
         "--correctCells",
         "<list of cells>",
         "--group",
         "<group name>=<group file>",
         "--group",
         "<group name>=<group file>"
       )}
      |
    """.stripMargin
}
