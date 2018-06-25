# TenxKit


#### Tools - CellReads

This tool will generate a histogram of reads per cell barcode.
This can be used to validate output from cellranger or to set a alternative cutoff.
    
        

#### Tools - CellVariantcaller

This tool will call variants based on 10x data. Usually the output of cellranger is used.
Each cell will be treated a separated sample.
    
        

#### Tools - CalulateDistance

This tool will calculate for each cell combination a sum of distances.
The distances are the relative distance to the middle line for each allele divided by the total coverage on that position.
    
        

#### Tools - GroupDistance

This tool we try to group distances together. The result should be a clusters of 1 single sample.

This tool will execute multiple iterations to find the groups.
    
        

#### Tools - MergeBams

This tool can merge separated 10x experiments into a single bam file. This is used to simulate a mixed run.
This is used as a control for the GroupDistance tool.
    
        

#### Tools - EvalSubGroups

This tool will compare all given groups.
The histogram for a Correct match should be a steep histogram starting at 0.
When multiple peaks are seen this might be a mixture of samples.
    
        

#### Tools - ExtractGroupVariants

This tool will merge variants from the same group. This will be a representation of the real samples.
This can also be used to validate if there is a true set known.
    
        

#### Tools - ExtractCellFastqs

This tools will extract fastq files for a given list of cell barcodes.
All reads that are marked as duplicate or secondary will be skipped by default.
    
        

# Documentation

For documentation and manuals visit our [github.io page](https://biopet.github.io/tenxkit).

# About


TenxKit is part of BIOPET tool suite that is developed at LUMC by [the SASC team](http://sasc.lumc.nl/).
Each tool in the BIOPET tool suite is meant to offer a standalone function that can be used to perform a
dedicate data analysis task or added as part of [BIOPET pipelines](http://biopet-docs.readthedocs.io/en/latest/).

All tools in the BIOPET tool suite are [Free/Libre](https://www.gnu.org/philosophy/free-sw.html) and
[Open Source](https://opensource.org/osd) Software.
    

# Contact


<p>
  <!-- Obscure e-mail address for spammers -->
For any question related to TenxKit, please use the
<a href='https://github.com/biopet/tenxkit/issues'>github issue tracker</a>
or contact
 <a href='http://sasc.lumc.nl/'>the SASC team</a> directly at: <a href='&#109;&#97;&#105;&#108;&#116;&#111;&#58;&#115;&#97;&#115;&#99;&#64;&#108;&#117;&#109;&#99;&#46;&#110;&#108;'>
&#115;&#97;&#115;&#99;&#64;&#108;&#117;&#109;&#99;&#46;&#110;&#108;</a>.
</p>

     

