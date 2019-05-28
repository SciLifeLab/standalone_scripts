#!/bin/bash

###
### This script is to zip and save certain directories from Google Drive which have been backed up to 'ngi.transfer' to the homer system for QA purposes
### This script requires that The Google Backup and Sysnc utility be running on the ngi.transfer computer and that ngi.transfer has access to homer
### Recommended to be run once a month.
###

currentDate=`date +"%Y-%m-%d"`
echo 'Run $currentDate'
#Zip GDrive Sync file
zip -vr $HOME/opt/zipFilesTemp/QAbackup.$currentDate.zip  $HOME/Google\ Drive/

#make mountpoint
mkdir -p $HOME/kvalitetssystem

#Mount homer password should be in keychain
mount_smbfs //ngi.transfer@homer.scilifelab.se/kvalitetssystem $HOME/kvalitetssystem

#Copy zipped backup file
rysnc -av $HOME/opt/zipFilesTemp/QAbackup.$currentDate.zip $HOME/kvalitetssystem

#Unmount homer
umount $HOME/kvalitetssystem

#rm zip file
rm $HOME/opt/zipFilesTemp/QAbackup.$currentDate.zip
