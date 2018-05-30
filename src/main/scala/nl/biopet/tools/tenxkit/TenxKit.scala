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

import nl.biopet.tools.tenxkit.calculatedistance.CalulateDistance
import nl.biopet.tools.tenxkit.cellreads.CellReads
import nl.biopet.tools.tenxkit.evalsubgroups.EvalSubGroups
import nl.biopet.tools.tenxkit.extractgroupvariants.ExtractGroupVariants
import nl.biopet.tools.tenxkit.groupdistance.GroupDistance
import nl.biopet.tools.tenxkit.mergebams.MergeBams
import nl.biopet.tools.tenxkit.samplematcher.SampleMatcher
import nl.biopet.tools.tenxkit.variantcalls.CellVariantcaller
import nl.biopet.utils.tool.ToolCommand
import nl.biopet.utils.tool.multi.MultiToolCommand

object TenxKit extends MultiToolCommand {

  override def urlToolName = "10xkit"

  def subTools: Map[String, List[ToolCommand[_]]] =
    Map(
      "Tool" -> List(SampleMatcher,
                     CellReads,
                     CellVariantcaller,
                     CalulateDistance,
                     GroupDistance,
                     MergeBams,
                     EvalSubGroups,
                     ExtractGroupVariants))

  def descriptionText: String = extendedDescriptionText

  def manualText: String = extendedManualText

  def exampleText: String = extendedExampleText

  override def extendedUsage = true

}
