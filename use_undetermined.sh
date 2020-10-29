#!/bin/bash

set -e

if [ -z "$1" ]
  then
    echo "ERROR: No flowcelll specified"
    echo "Usage:" $0 " <flowcell> <lane> <sample>"
    echo "Example: " $0 "160901_ST-E00214_0087_BH33GHALXX 1 P4601_273"
    exit 1
elif [ -z "$2" ]
  then
    echo "No lane specified"
    echo "Usage:" $0 " <flowcell> <lane> <sample>"
    echo "Example: " $0 "160901_ST-E00214_0087_BH33GHALXX 1 P4601_273"

    exit 1
elif [ -z "$3" ]
  then
    echo "No sample specified"
    echo "Usage:" $0 " <flowcell> <lane> <sample>"
    echo "Example: " $0 "160901_ST-E00214_0087_BH33GHALXX 1 P4601_273"

    exit 1
fi
flowcell=$1
lane=$2
sample=$3

current_pwd=$PWD
sample_dir="/proj/ngi2016003/incoming/$flowcell/Demultiplexing/*/Sample_$sample"
echo 'sample_dir:' $sample_dir
cd $sample_dir

undetermined="../../"$sample"_Undetermined_L01"$lane"_R*.fastq.gz"
for i in $undetermined; do ln -s $i .; done


project=$(echo $sample | cut -f1 -d '_')
echo "To (re-)organize flowcell and start analysis run the following commands:"
echo "  ngi_pipeline_start.py organize flowcell /proj/ngi2016003/incoming/$flowcell -p $project"
echo "  ngi_pipeline_start.py analyze project $project -f"
