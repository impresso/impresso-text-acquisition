The file "kranten_pd_voorbeeld.zip" contains the full texts (OCR, ALTO, XML) of  5 newspaper editions from the period 1618 - 1876. Using this small test file you can explore the structure of the much larger zip files in the Delpher open newspaper archive. If you feel comfortable with the data in this test file, you can download the full zip files (0,4-14 GB) from the Delpher open newspaper archive.

Permantent URL of this zip test file: http://resolver.kb.nl/resolve?urn=DATA:kranten:kranten_pd_voorbeeld.zip

=== Delpher open newspaper archive ===
This zipfile is part of the Delpher open newspaper archive. To find out about this archive and the zipfiles it contains, visit http:///www.delpher.nl/data (choose "Download teksten als zip-bestanden --> Delpher open krantenarchief"). Please note: this information is available in Dutch only for the time being, but Delpher is working on providing these pages in English a.s.a.p.

=== Exploring the data ===
The zip test file contains the full texts (OCR, ALTO, XML) of 5 newspaper editions:
* Courante uyt Italien, Duytslandt, &c.  from  14-06-1618. See in Delpher: http://resolver.kb.nl/resolve?urn=ddd:010500649  
* Oprechte Haerlemsche courant   from 22-12-1685. See in Delpher: http://resolver.kb.nl/resolve?urn=ddd:011225333
* 's Gravenhaegse courant  from 15-05-1750. See in Delpher: http://resolver.kb.nl/resolve?urn=ddd:010724973
* Advertentieblad, bekendmakingen en onderscheidene berigten van Groningen from 16-11-1813. See in Delpher: http://resolver.kb.nl/resolve?urn=ddd:010179815
* Algemeen Handelsblad  from 01-12-1876. See in Delpher: http://resolver.kb.nl/resolve?urn=ddd:010100993

===Structure of the zip test file ===
The 5 editions in "kranten_pd_voorbeeld.zip" are structured as follows:

 * Folder per year: 1618, 1685, 1750, 1813, 1876
 * * Folder per month (based on month numbers, 01=January, 12=December. So: 06, 12, 05, 11, 12)
 * * * Folder per day (based on day numbers. So: 14, 22, 15, 16, 01)
 * * * * Folder based on a unique identifier, starting with "DDD_ddd_". In the folder you will find: 
 * * * * * Metadata coded into a MPEG21-DIDL file, per newspaper edition (didl.xml)
 * * * * * Text coded into (uncorrected) OCR files, per newspaper article (file name ends with _articletext.xml). 
 * * * * * Word coordinates coded into ALTO files, per newspaper page (file name ends with _alto.xml). 

To keep its download size manageable this zip test file does NOT contain the newspaper page scans (JP2 files) and editions in PDF format you can find on the Delpher site. The metadata (didl.xml) contains persistent URLs of the JP2 files (syntax: http://resolver.kb.nl/resolve?urn=<identifier>:<pagenumber>:image) and PDF files (syntax: http://resolver.kb.nl/resolve?urn=<identifier>:pdf). From these URLs you can download the files yourself.

=== Questions, remarks, suggestions? ===  
Delpher would like to hear them via dataservices@kb.nl

Last update: 16-01-2017
