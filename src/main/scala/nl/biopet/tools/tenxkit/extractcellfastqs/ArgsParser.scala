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

package nl.biopet.tools.tenxkit.extractcellfastqs

import java.io.File

import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}

class ArgsParser(toolCommand: ToolCommand[Args])
    extends AbstractOptParser[Args](toolCommand) {
  opt[File]('i', "inputFile")
    .required()
    .action((x, c) => c.copy(inputFile = x))
    .text("Input bam file")
  opt[File]('R', "reference")
    .required()
    .action((x, c) => c.copy(reference = x))
    .text("Reference fasta file")
  opt[File]('o', "outputDir")
    .required()
    .action((x, c) => c.copy(outputDir = x))
    .text("Input bam file")
  opt[File]("correctCells")
    .required()
    .action((x, c) => c.copy(correctCells = x))
    .text("List of correct cells")
  opt[String]('s', "sampleTag")
    .action((x, c) => c.copy(sampleTag = x))
    .text(
      s"Tag where to find the sample barcode, default '${Args().sampleTag}'")
  opt[Int]('s', "binSize")
    .action((x, c) => c.copy(binSize = x))
    .text(s"Bin size per partition, default '${Args().binSize}'")
  opt[File]("intervals")
    .action((x, c) => c.copy(intervals = Some(x)))
    .text("Bed file of regions to analyse")
  opt[String]("sparkMaster")
    .required()
    .action((x, c) => c.copy(sparkMaster = x))
    .text("Spark master")
}
