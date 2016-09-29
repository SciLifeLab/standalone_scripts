#!/usr/bin/env python
"""
zendesk_attachment_backup.py
Takes an XML ticket dump from ZenDesk and parses out URLs
that match specified patterns. These URLs can either be printed
to a file (using the -p option) or downloaded to a local directory.
"""

from __future__ import print_function
import argparse
import os
import urllib2
import re
import zipfile
import sys

def collect_urls(input_files, print_urls, **kwargs):
   """ Collect delivery report URLs and print to file
   """
   # Setup
   url_prefix = 'https://ngisweden.zendesk.com/attachments/token/[^>]+'
   re_patterns = [
      '_sample_summary.pdf',
      '_sample_summary.html',
      '_project_summary.pdf',
      '_project_summary.html',
      '_lanes_info.txt',
      '_sample_info.txt',
      '_library_info.txt',
   ]
   urls = set()
   
   # Go through each supplied file
   for fn in input_files:
      if not kwargs.get('quiet'):
          print("Loading {}".format(fn))
      # Load the XML file
      tickets_xml = None
      if fn.endswith('.zip'):
         try:
            zendesk_zip = zipfile.ZipFile(fn)
         except Exception as e:
            print("Couldn't read '{}' - Bad zip file".format(fn))
            continue
         for f in zendesk_zip.namelist():
            if f.endswith('tickets.xml'):
               with zendesk_zip.open(f) as fh:
                  tickets_xml = fh.read().decode('utf8')
      
      else:
         with open(fn,'r') as f_h:
             tickets_xml = f_h.read()

      # Get matches
      if tickets_xml is not None:
         for pattern in re_patterns:
            urls.update(re.findall(url_prefix + pattern, tickets_xml))
      
      if not kwargs.get('quiet'):
          print("  Found {} unique URLs so far..".format(len(urls)))
      
   
   # Check we have some URLs
   if len(urls) == 0:
      print("Error - no URLs found")
      sys.exit(1)
   
   # Make nice filenames
   downloads = dict()
   for url in urls:
      fn = os.path.basename(url).replace('?name=','')
      downloads[fn] = url

   # Print URLs to file if requested
   if print_urls:
      target_file=os.path.join(kwargs.get("output_dir"),'attachment_urls.txt')
      with open(target_file, "w") as fh:
         for url in urls:
            fh.write("{}\n".format(url))
      if not kwargs..get('quiet'):
          print("Printed results to {}.txt".format(target_file))

   return downloads

def download_files(downloads, output_dir, force_overwrite, **kwargs):
    """ Save files to disk. Input: Dict with desired filename as key, URL as value.
    """
    # Make the directory if we need to
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
   
    # Check if any files exist and skip them if so
    num_dls = len(downloads)
    if not force_overwrite:
        downloads = {fn:url for fn,url in downloads.iteritems() if not os.path.exists(os.path.join(output_dir, fn))}
        if num_dls != len(downloads) and not kwargs.get('quiet'):
            print("Skipping {} files as already downloaded.".format(num_dls - len(downloads)))

   # Loop through the downloads and get them one at a time
    num_dls = len(downloads)
    i = 1
    for fn, url in downloads.iteritems():
        path = os.path.join(output_dir, fn)
        if not kwargs.get('quiet'):
            print("Downloading {} of {} - {}".format(i, num_dls, fn))
    for _try in xrange(3):
        try:
            dl = urllib2.urlopen(url)
            with open(path, 'wb') as fh:
                fh.write(dl.read())
            break
        except urllib2.URLError:
            print("Error downloading {} on try {}".format(url,_try))
            sys.exit(1)

        dl.close()
     i += 1


if __name__ == "__main__":
   # Command line arguments
   parser = argparse.ArgumentParser("Get delivery report attachment URLs from ZenDesk XML dump")
   parser.add_argument("-o", "--output_dir", dest="output_dir", default='downloads',
                        help="Directory to save attachments to. Default: downloads/")
   parser.add_argument("-p", "--print_urls", dest="print_urls", action='store_true',
                        help="Save URLs to file 'attachment_urls.txt' instead of downloading")
   parser.add_argument("-f", "--force_overwrite", dest="force_overwrite", action='store_true',
                        help="Overwrite existing files. Default: Don't download if file exists.")
   parser.add_argument("-q", "--quiet", dest="quiet", action='store_true',
                        help="Keep quiet like the good cronjob you are.")
   parser.add_argument('input_files', nargs='+',
                        help="Path to ZenDesk tickets.xml export.")
   kwargs = vars(parser.parse_args())

   # Get the URLs
   downloads = collect_urls(**kwargs)

   # Download the URLs
   if not kwargs['print_urls']:
      download_files(downloads, **kwargs)
