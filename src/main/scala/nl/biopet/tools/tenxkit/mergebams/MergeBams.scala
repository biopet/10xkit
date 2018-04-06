package nl.biopet.tools.tenxkit.mergebams

import java.io.{File, PrintWriter}

import htsjdk.samtools.{SAMFileHeader, SAMFileWriterFactory, SamReaderFactory}
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
          .foreach(writer.println(_))
    }
    writer.close()
  }

  def descriptionText: String =
    """
      |
    """.stripMargin

  def manualText: String =
    """
      |
    """.stripMargin

  def exampleText: String =
    """
      |
    """.stripMargin

}
