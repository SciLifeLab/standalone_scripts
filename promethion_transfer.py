""" Transfers new PromethION runs to ngi-nas using rsync.
"""

import os
import shutil
import argparse
import subprocess

def main(args):
    """Find promethion runs and transfer them to storage. 
    Archives the run when the transfer is complete."""
    data_dir = args.source_dir
    destination_dir = args.dest_dir
    archive_dir = args.archive_dir
    log_file = os.path.join(data_dir, 'rsync_log.txt')
    runs = [os.path.join(data_dir, top_dir) for top_dir in os.listdir(data_dir)
            if os.path.isdir(os.path.join(data_dir, top_dir))]
    
    # Split finished and unfinished runs
    not_finished = []
    finished = []
    
    for run in runs:
        if sequencing_finished(run):
            finished.append(run)
        else:
            not_finished.append(run)

    # Start transfer of unfinished runs first (detatched)
    for run in not_finished:
        sync_to_storage(run, destination_dir, log_file)
    for run in finished:
        final_sync_to_storage(run, destination_dir, archive_dir, log_file) 
        

def sequencing_finished(run_dir):
    sequencing_finished_indicator = 'sequencing_summary'
    run_dir_content = os.listdir(run_dir)
    for item in run_dir_content:
        if sequencing_finished_indicator in item:
            return True
    return False

def sync_to_storage(run_dir, destination, log_file):
    """Sync the run to storage using rsync. 
    Skip if rsync is already running on the run."""
    command = ['run-one', 'rsync', '-rv', '--log-file=' + log_file, run_dir, destination] #TODO: might be an issue if multiple rsyncs write to the same log file at the same time
    process_handle = subprocess.Popen(command)
    print('Initiated rsync with the following parameters: {}'.format(command))
    
def final_sync_to_storage(run_dir, destination, archive_dir, log_file):
    """Do a final sync of the run to storage, then archive it. 
    Skip if rsync is already running on the run."""
    print('Performing a final sync of {} to storage'.format(run_dir))
    command = ['run-one', 'rsync', '-rv', '--log-file=' + log_file, run_dir, destination]
    process_handle = subprocess.run(command)
    if process_handle.returncode == 0:
        archive_finished_run(run_dir, archive_dir)
    else:
        print('Previous rsync might be running still. Skipping {} for now.'.format(run_dir))
        return

def archive_finished_run(run_dir, archive_dir):
    """Move finished run to archive (nosync)."""
    print('archiving {}'.format(run_dir))
    shutil.move(run_dir, archive_dir)

if __name__ == "__main__":
    # This is clunky but should be fine since it will only ever run as a cronjob
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', dest='source_dir', help='Full path to directory containing runs to be synced.')
    parser.add_argument('--dest', dest='dest_dir', help='Full path to destination directory to sync runs to.')
    parser.add_argument('--archive', dest='archive_dir', help='Full path to directory containing runs to be synced.')
    args = parser.parse_args()
    
    main(args)