### Some notes about BL sample

**From Yann Ryan email (19.05.2019)**

BLIP_20190325_03 – BLIP stands for British Library Ingest Package. A BLIP contains a series of newspaper issues sent by the digitisation company. The format is ‘BLIP_’ + date of transfer + ‘_’ + identifier. The BLIP folder contains a series of tarballs, each for an individual issue. The tar files are named: NLP + ‘_’ + date of issue + ‘.tar’



```
|
0002364 – Folder name for a single issue, based on the NLP, a unique ID given to each title before digitisation.
|
 1811  - Year of publication
 1812
  ..
             |
             0127 – Month/Date of publication
              0203
              |
              --------- 0002364_18110127_0001.jp2 Filename structure is NLP + ‘_’ + date of issue + ‘_’ + page number + ‘.jp2’
              --------- 0002364_18110127_0001.xml ALTO files. Filename structure is NLP + ‘_’ + date of issue + ‘_’ + page number + ‘.xml’. See attached zip.
              ---------- 0002364_18110127_0002.xml etc.
              ---------- 0002364_18110127_mets.xml METS file. NLP + ‘_’ + date of issue + ‘_mets.xml’
              ————— 0002364_18110127_manifest.txt
```



The directory at the top of the tree is the NLP for the issue. Subsequent sub folders give the year of publication followed by the month and day of publication.

The image for each page is in the JPG2000 format and has the file extension ‘.jp2’. Page filenames are composed as follows:
`NLP + ‘_’ + date of issue + ‘_’ + page number + ‘.jp2’` 

The OCR text for each page uses the ALTO XML standard and has the file extension ‘.xml’. ALTO text file names are:
`NLP + ‘_’ + date of issue + ‘_’ + page number + ‘.xml’`

The METS file for each page adheres to a profile designed by the BL and has the file extension ‘.xml’. The METS file name is as follows:
`NLP + ‘_’ + date of issue + ‘_mets.xml’`

Finally, the manifest file for the issue is in the same format as the BLIP manifest and contains a list of files in the issue along with a unique fingerprint for each file. This manifest is sometimes referred to as the low level manifest.

The format of the issue manifest is as follows:
`‘BLIP_’ + date of transfer + ‘_manifest.txt’`



**From Matteo's answer on 24.05.2019** (including only what's relevant)

… There are some substantial differences between your data format and the one of the BNL, meaning it will be necessary to write an ad-hoc data importer. The most important difference is the way the logical structure is managed: you are using the <mets:smLinkGrp> element to achieve that (e.g. the various parts of an article are brought back together), whereas the BNL format relies exclusively on the   <structMap> element… 







