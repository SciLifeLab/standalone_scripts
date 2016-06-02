###
### This script allows to easily download from preproc1 the informaiton that is usually
### requested by Illumina tech support in order to troubleshoot an HiSeqX run
###


if [ $# -eq 1 ]; then
    echo "Trying to retrive information for FC $0"
else
    echo "You need to specify one FC (DATE_INSTRUM_RUN_FCID)"
fi

FC="$1"


if [ -d $FC ]; then
    echo "FC $FC exists: aborting, delete before rerunning this"
    exit 1
fi

mkdir $FC

echo "  fetch xml info"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/*xml $FC > $FC\_sync.out 2> $FC\_sync.err

echo "   fetch csv info"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/*csv $FC  >> $FC\_sync.out 2>> $FC\_sync.err

echo "   fetch std out and std err of commands (bcl2fastq and rsync)"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/*.err $FC >> $FC\_sync.out 2>> $FC\_sync.err
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/*.out $FC >> $FC\_sync.out 2>> $FC\_sync.err

echo "   fetch Logs dir"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/Logs $FC >> $FC\_sync.out 2>> $FC\_sync.err

echo "    fetch InterOp dir"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/InterOp $FC >> $FC\_sync.out 2>> $FC\_sync.err

echo "    fetch Config dir"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/Config $FC >> $FC\_sync.out 2>> $FC\_sync.err

echo "    fetch RTALogs dir"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/RTALogs $FC >> $FC\_sync.out 2>> $FC\_sync.err

echo "    fetch Recipe dir"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/Recipe $FC >> $FC\_sync.out 2>> $FC\_sync.err

echo "    fetch Demux dir"
scp -r francesco.vezzi@preproc1.scilifelab.se:/srv/illumina/HiSeq_X_data/nosync/$FC/Demultiplexing $FC >> $FC\_sync.out 2>> $FC\_sync.err

tar -zcvf $FC.tar.gz $FC >> $FC\_sync.out 2>> $FC\_sync.err

echo "The $FC.tar.gz contains:"
echo " - RunInfo.xml"
echo " - RTAConfiguration.xml"
echo " - runParameters.xml"
echo " - RTAConfiguration.xml"
echo " - SampleSheet.csv"
echo " - bcl2fastq.err (std err of bcl2fastq command)"
echo " - bcl2fastq.out (std out of bcl2fastq command)"
echo " - InterOp folder"
echo " - Logs folder"
echo " - Config folder"
echo " - RTALogs folder"
echo " - Recipe folder"
echo " - Demultiplexing folder"
echo ""


exit 0
