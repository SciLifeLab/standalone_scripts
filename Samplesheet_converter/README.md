# Samplesheet_converter
### v1.0
### Written by Chuan Wang (chuan-wang@github), 2017-03-06
#### These scripts are for the purpose of converting Illumina samplesheet that contains Chromium 10X indexes for demultiplexing.
#### Headers and lines with ordinary indexes will be passed without any change. Lines with Chromium 10X indexes will be expanded into 4 lines, with 1 index in each line, and suffix 'Sx' will be added at the end of sample names.
### Example:
Script:
```
python main.py -i <inputfile> -o <outputfile> -x <indexlibrary>
```
Original samplesheet:
```
[Header]
Investigator Name,Chuan Wang
Date,None
Experiment Name,Project_001
[Reads]
151
151
[Data]
Lane,SampleID,SampleName,SamplePlate,SampleWell,index,index2,Project,Description
1,Sample_101,101,HGWT5ALXX,1:1,SI-GA-A1,,Project_001,
1,Sample_102,102,HGWT5ALXX,1:1,SI-GA-B1,,Project_001,
2,Sample_103,103,HGWT5ALXX,2:1,SI-GA-C1,,Project_001,
```
Modified samplesheet:
```
[Header]
Investigator Name,Chuan Wang
Date,None
Experiment Name,Project_001
[Reads]
151
151
[Data]
Lane,SampleID,SampleName,SamplePlate,SampleWell,index,index2,Project,Description
1,Sample_101_S1,101_S1,HGWT5ALXX,1:1,GGTTTACT,,Project_001,
1,Sample_101_S2,101_S2,HGWT5ALXX,1:1,CTAAACGG,,Project_001,
1,Sample_101_S3,101_S3,HGWT5ALXX,1:1,TCGGCGTC,,Project_001,
1,Sample_101_S4,101_S4,HGWT5ALXX,1:1,AACCGTAA,,Project_001,
1,Sample_102_S1,102_S1,HGWT5ALXX,1:1,GTAATCTT,,Project_001,
1,Sample_102_S2,102_S2,HGWT5ALXX,1:1,TCCGGAAG,,Project_001,
1,Sample_102_S3,102_S3,HGWT5ALXX,1:1,AGTTCGGC,,Project_001,
1,Sample_102_S4,102_S4,HGWT5ALXX,1:1,CAGCATCA,,Project_001,
2,Sample_103_S1,103_S1,HGWT5ALXX,2:1,CCACTTAT,,Project_001,
2,Sample_103_S2,103_S2,HGWT5ALXX,2:1,AACTGGCG,,Project_001,
2,Sample_103_S3,103_S3,HGWT5ALXX,2:1,TTGGCATA,,Project_001,
2,Sample_103_S4,103_S4,HGWT5ALXX,2:1,GGTAACGC,,Project_001,
```
