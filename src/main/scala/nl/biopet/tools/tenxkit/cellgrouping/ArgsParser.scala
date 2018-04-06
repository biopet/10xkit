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

package nl.biopet.tools.tenxkit.cellgrouping

import java.io.File

import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}

class ArgsParser(toolCommand: ToolCommand[Args])
    extends AbstractOptParser[Args](toolCommand) {
  opt[File]('i', "inputFile")
    .required()
    .action((x, c) => c.copy(inputFile = x))
    .text("Input bam file")
  opt[File]('R', "referenceFasta")
    .required()
    .action((x, c) => c.copy(reference = x))
    .text("Reference fasta")
  opt[File]('o', "outputDir")
    .required()
    .action((x, c) => c.copy(outputDir = x))
    .text("Output directory")
  opt[File]("correctCells")
    .required()
    .action((x, c) => c.copy(correctCells = x))
    .text("List of correct cells")
  opt[Int]("minAlleleCoverage")
    .action((x, c) => c.copy(minAlleleCoverage = x))
    .text("Minimal coverage for each allele")
  opt[Double]("minTotalAltRatio")
    .action((x, c) => c.copy(minTotalAltRatio = x))
    .text(
      s"Filter out variants with a total alt ratio below this value, default: ${Args().minTotalAltRatio}")
  opt[File]("intervals")
    .action((x, c) => c.copy(intervals = Some(x)))
    .text("Regions to analyse, if not given whole genome is done")
  opt[String]('s', "sampleTag")
    .action((x, c) => c.copy(sampleTag = x))
    .text(
      s"Tag where to find the sample barcode, default '${Args().sampleTag}'")
  opt[String]('u', "umiTag")
    .action((x, c) => c.copy(umiTag = Some(x)))
    .text(
      s"Tag where to find the umi barcode, if not given read with the duplicate flag will be ignored'")
  opt[Int]("binSize")
    .action((x, c) => c.copy(binSize = x))
    .text(s"Size of each bin: default ${Args().binSize} base pairs")
  opt[Unit]("writeScatters")
    .action((_, c) => c.copy(writeScatters = true))
    .text(s"Write fractions for all sample pairs")
  opt[String]("sparkMaster")
    .required()
    .action((x, c) => c.copy(sparkMaster = x))
    .text("Spark master")
}
