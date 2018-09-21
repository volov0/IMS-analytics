#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Copyright (c) 2018 CA.  All rights reserved.
Created on Thu Jul 12 15:29:35 2018
@author: Vaclav Koudelka
@email: vaclav.koudelka@ca.com
This is a script for downloading seguentail variable length log files from mainframe.
Problem during ftp download of a binary variable length file is that record lengths
are lost. This is why I first download the file in "text" transfer mode which delimits
each record with new line delimeter. From such file can be extracted array of record 
lengths. After that binary download follows, resulting file is stored in file system
with LogDownloader class. Resulting files are in binary format where each record is 
delimited by custom delimeter defined in LogDownloader.py.

Execute with  python -O ftpdown.py
'''

SITE = 'host'
USER = 'login'
PASS = 'password'
DOWNLOAD_LIST = 'conf/download_list.txt'
BIN_DEST = 'slds/'


from logdownloader import LogDownloader
import ftplib
import os, sys


def storbin(block,fo):
    fo.write(block)

def stortxt_obsolete(block,fo):
    bb = (block+'aa').encode('latin-1')
    fo.write(bb)
        
def download_bin(fn, lrecs):
    '''FTP download of a binary file'''
    quoted_fn = '\''+fn+'\''
    print('Downloading bin '+quoted_fn, end='')
    sys.stdout.flush()
    error = False
    with open(BIN_DEST+fn, 'wb') as fob:
        try: 
            ld = LogDownloader(fob, lrecs)
            ftp.retrbinary('RETR '+quoted_fn, lambda block: ld.write(block))
            ld.done()
            print(' done.', end='\n')
        except ftplib.error_perm as e:
            error = True
            print(' Error: '+str(e), end='\n')
    sys.stdout.flush()
    if error: os.remove(BIN_DEST+fn)

def retrieve_record_lens(fn):
    '''FTP download of a text file
    It needs to run modified ftp.retrlines() to work correctly.
    This modification lives in C:\Program Files\Python35\Lib, it supports newline parameter,
    which is needed because standard implementation works with \n but it can be 
    misplaced easily. Passing \r\n fixes the problem.
    This functions returns a list of dataset record lengths.
    '''
    quoted_fn = '\''+fn+'\''
    print('Downloading txt '+quoted_fn, end='')
    sys.stdout.flush()
    lrecs = []
    try: 
        ftp.retrlines('RETR '+quoted_fn, lambda block: lrecs.append(len(block)), newline='\r\n')
        print(' done - record lengths retrieved, record count = {}, max record = {}.'.format(len(lrecs), max(lrecs)), end='\n')
    except ftplib.error_perm as e:
        print(' Error: '+str(e), end='\n')
    sys.stdout.flush()

    return lrecs


with open(DOWNLOAD_LIST, 'r') as f:
    try:
        ftp = ftplib.FTP(SITE)
        ftp.login(USER, PASS)
        print('FTP connected: {}'.format(ftp.getwelcome()))
        print('FTP encoding: {}'.format(ftp.encoding))
        for line in f:
            if (len(line.strip()) == 0): continue
            if (os.path.isfile(BIN_DEST+line.strip())): 
                print('Skipping '+line.strip())
                continue
            lrecs = retrieve_record_lens(line.strip())
            if (len(lrecs) > 0):
                download_bin(line.strip(), lrecs)
    except ftplib.all_errors as e:
        print('FTP Error:'+str(e))
    finally:
        ftp.quit()     
        print('FTP disconnected.')
