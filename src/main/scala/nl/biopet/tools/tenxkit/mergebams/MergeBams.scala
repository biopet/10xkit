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

import java.io.{File, PrintWriter}

import htsjdk.samtools.{SAMFileHeader, SAMFileWriterFactory, SamReaderFactory}
import nl.biopet.tools.tenxkit.TenxKit
import nl.biopet.utils.tool.ToolCommand
import nl.biopet.utils.io

object MergeBams extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    require(cmdArgs.bamFiles.size > 1, "At least 2 bam files should be given")
    require(cmdArgs.bamFiles.keys.forall(k => cmdArgs.barcodes.contains(k)),
            "Not all given bam files have a barcode file")
    require(cmdArgs.barcodes.keys.forall(k => cmdArgs.bamFiles.contains(k)),
            "Not all given barcode files have a bam file")

    logger.info("Start")
    prefixBarcodes(cmdArgs.barcodes, cmdArgs.outputBarcodes)
    val bamReaders = cmdArgs.bamFiles.map(x =>
      x._1 -> SamReaderFactory.makeDefault().open(x._2))
    val oldHeaders = bamReaders.map(x => x._2.getFileHeader)
    require(
      oldHeaders.forall(_.getSortOrder == SAMFileHeader.SortOrder.coordinate),
      "Not all files are coordinate sorted")
    val dict = {
      val dicts = oldHeaders.map(_.getSequenceDictionary)
      dicts.tail.foreach(_.assertSameDictionary(dicts.head))
      dicts.head
    }
    val header = new SAMFileHeader()
    header.setSequenceDictionary(dict)
    header.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    val writer =
      new SAMFileWriterFactory().makeBAMWriter(header, true, cmdArgs.outputBam)

    val it = new PrefixIterator(bamReaders, dict, cmdArgs.sampleTag)
    it.foreach(writer.addAlignment)

    writer.close()
    bamReaders.foreach(_._2.close())
    logger.info("Done")
  }

  /** This method will prefix the barcodes with the sampleId */
  def prefixBarcodes(files: Map[String, File], outputFile: File): Unit = {
    val writer = new PrintWriter(outputFile)
    files.foreach {
      case (sample, file) =>
        io.getLinesFromFile(file)
          .map(sample + "-" + _)
          .foreach(writer.println)
    }
    writer.close()
  }

  def descriptionText: String =
    """
      |This tool can merge separated 10x experiments into a single bam file. This is used to simulate a mixed run.
      |This is used as a control for the GroupDistance tool.
    """.stripMargin

  def manualText: String =
    """
      |This tool need at least two bam files and each bam file should also come with a barcode file.
      |The cell barcodes will be prefixed by the sample name.
    """.stripMargin

  def exampleText: String =
    s"""
      |A default run:
      |${TenxKit.example(
         "MergeBams",
         "-i",
         "<sample1>=<input bamfile 1>",
         "--inputBarcode",
         "<sample1>=<barcodes 1>",
         "-i",
         "<sample2>=<input bamfile 2>",
         "--inputBarcode",
         "<sample2>=<barcodes 2>",
         "-o",
         "<output bam file>",
         "-b",
         "<output barcodes>"
       )}
      |
    """.stripMargin

}
