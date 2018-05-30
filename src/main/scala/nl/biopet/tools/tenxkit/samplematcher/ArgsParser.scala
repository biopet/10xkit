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

package nl.biopet.tools.tenxkit.samplematcher

import java.io.File

import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}

class ArgsParser(toolCommand: ToolCommand[Args])
    extends AbstractOptParser[Args](toolCommand) {
  opt[File]('i', "inputFile")
    .required()
    .action((x, c) => c.copy(inputFile = x))
    .text("Input bam file")
  opt[File]('o', "outputDir")
    .required()
    .action((x, c) => c.copy(outputDir = x))
    .text("Output directory")
  opt[File]('R', "reference")
    .required()
    .action((x, c) => c.copy(reference = x))
    .text("Reference fasta file")
  opt[File]("correctCells")
    .required()
    .action((x, c) => c.copy(correctCellsFile = x))
    .text("List of correct cells")
  opt[Int]('c', "expectedSamples")
    .required()
    .action((x, c) => c.copy(expectedSamples = x))
    .text("Number of expected samples")
  opt[Int]("partitions")
    .action((x, c) => c.copy(partitions = Some(x)))
    .text("Number of init partitions, default: size of the bam divided by 10M")
  opt[String]("sampleTag")
    .action((x, c) => c.copy(sampleTag = x))
    .text(
      s"Tag where to find the cell barcode in the bamfile, default: ${Args().sampleTag}")
  opt[String]("umiTag")
    .action((x, c) => c.copy(umiTag = x))
    .text(
      s"Tag where to find the umi barcode in the bamfile, default: ${Args().umiTag}")
  opt[File]("intervals")
    .action((x, c) => c.copy(intervals = Some(x)))
    .text("Bed file of regions to analyse")
  opt[Double]("seqError")
    .action((x, c) => c.copy(seqError = x.toFloat))
    .text(s"Sequence error rate. Default: ${Args().seqError}")
  opt[Double]("maxPvalue")
    .action((x, c) => c.copy(cutoffs = c.cutoffs.copy(maxPvalue = x.toFloat)))
    .text("Maximum allowed pvalue on seq error")
  opt[Int]("minAlternativeDepth")
    .action((x, c) => c.copy(cutoffs = c.cutoffs.copy(minAlternativeDepth = x)))
    .text("Min alternative depth")
  opt[Int]("minCellAlternativeDepth")
    .action((x, c) =>
      c.copy(cutoffs = c.cutoffs.copy(minCellAlternativeDepth = x)))
    .text("Min alternative depth for atleast 1 cell")
  opt[Int]("minTotalDepth")
    .action((x, c) => c.copy(cutoffs = c.cutoffs.copy(minTotalDepth = x)))
    .text("Minimal total depth")
  opt[Char]("minBaseQual")
    .action((x, c) => c.copy(cutoffs = c.cutoffs.copy(minBaseQual = x.toByte)))
    .text("Minimal base quality")
  opt[String]("sparkMaster")
    .required()
    .action((x, c) => c.copy(sparkMaster = x))
    .text("Spark master")
}
