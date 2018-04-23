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

package nl.biopet.tools.tenxkit.mergebams

import java.io.File

import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}

class ArgsParser(toolCommand: ToolCommand[Args])
    extends AbstractOptParser[Args](toolCommand) {
  opt[(String, File)]('i', "inputBam")
    .required()
    .unbounded()
    .action {
      case ((sample, bamfile), c) =>
        c.copy(bamFiles = c.bamFiles + (sample -> bamfile))
    }
    .text("Input bam file")
  opt[(String, File)]("inputBarcode")
    .required()
    .unbounded()
    .action {
      case ((sample, file), c) =>
        c.copy(barcodes = c.barcodes + (sample -> file))
    }
    .text("Input barcodes file")
  opt[File]('o', "outputBam")
    .required()
    .action((x, c) => c.copy(outputBam = x))
    .text("Output bam file")
  opt[File]('b', "outputBarcodes")
    .required()
    .action((x, c) => c.copy(outputBarcodes = x))
    .text("output barcodes file")
  opt[String]('s', "sampleTag")
    .action((x, c) => c.copy(sampleTag = x))
    .text(
      s"Tag where to find the sample barcode, default '${Args().sampleTag}'")
  opt[String]('u', "umiTag")
    .action((x, c) => c.copy(umiTag = x))
    .text(s"Tag where to find the umi barcode, default '${Args().umiTag}'")
  opt[Double]("downsampleFraction")
    .action((x, c) => c.copy(downsampleFraction = x))
    .text("Downsample fraction")
  opt[Int]("duplets")
    .action((x, c) => c.copy(duplets = x))
    .text(s"Number of duplets to generate, default ${Args().duplets}")
  opt[Int]("seed")
    .action((x, c) => c.copy(seed = x))
    .text("Random seed")
}
