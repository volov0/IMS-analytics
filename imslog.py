#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Copyright (c) 2018 CA.  All rights reserved.
Created on Fri Jul 20 13:35:07 2018
@author: Vaclav Koudelka
@email: vaclav.koudelka@ca.com
Script for transforming downloaded raw log files into basic pandas dataframe
which are stored in local file system as  HDF files.
'''

import pandas as pd
import sys, os, psutil, time, datetime
import logspecs

LOG_PATH = 'slds/'
HDF_PATH = 'hdf5/logs.h5'

'''This my custom delimeter inserted during file download.
It is used to separate particular log records'''
DELIMETER = bytes((0xb2, 0x1d, 0x2d))

    
class IMSLogDataset:
    '''Class for handling the log files stored on harddrive'''
    def __init__(self, dsn):
        self.dsn = dsn
        self.content = bytearray()
        self.recs = None
        self.load_blob_to_memory()
        self.get_records()
        self.clear_blob_from_memory()
        
    def __del__(self):
        #print('IMSLogDataset deleted')
        pass
    
    def load_blob_to_memory(self):
        '''Reads th log file into memory'''
        with open(self.dsn, 'rb') as f:
            self.content = f.read()
            #print('File loaded - {} bytes'.format(len(self.content)))
    
    def clear_blob_from_memory(self):
        '''Memory saver - get rid of the binary content when it' not needed'''
        self.content = None

    def get_record_count(self):
        print('content count: {}'.format(sys.getrefcount(self.recs)))
        print('content size: {}'.format(sys.getsizeof(self.recs)))
        return len(self.get_records())
    
    def get_records(self):
        ''' Transform blob of binary data into records '''
        if self.recs is not None: return self.recs   # job was done before
        self.recs = []                               # initialize records
        if len(self.content) == 0: return self.recs  # don't bother with empty file

        pos = 0
        while True:
            fi = self.content.find(DELIMETER, pos)
            if (fi == -1):
                self.recs.append(self.content[pos:])
                break
            else:
                if (fi > pos):
                    self.recs.append(self.content[pos:fi])
                pos = fi + 3  # three for delimeter
        return self.recs
        
    def get_record_by_index(self, i):
        assert(i > len(self.recs))
        return self.recs[i]
    
    def get_hexpos_by_index(self, i):
        assert(i > len(self.recs))
        pos = 0
        for r in range(i): pos += len(self.recs[r]) + 3  # three is for delim
        return pos
    

# let's measuru exec time
start_time = time.time()
process = psutil.Process(os.getpid())

# init counter
count = 0

# initialize empty dataframe
ult_df = pd.DataFrame(data=[], columns=[])

# go through binary files in inuput directory and transform them into dataframe
for f in os.listdir(LOG_PATH):
    count += 1
    tlog = IMSLogDataset(LOG_PATH+f)
    tdf = pd.DataFrame(data=tlog.get_records(), columns=['blob'])
    tlog = None  # force him to vanish
        
    ult_df = ult_df.append(tdf)

    print('{} Memory usage total: {} Size of ult_df: {} Difference: {}'.format(count, process.memory_info().rss,
          sys.getsizeof(ult_df), process.memory_info().rss-sys.getsizeof(ult_df)))


for group, frame in ult_df.groupby('type'):
    print('Type {} has count {}'.format(group, frame.shape[0]))
    

# save final dataframe in hdf format
store = pd.HDFStore(HDF_PATH)
store['df'] = ult_df
store.close()

print('All done, execution time: {}'.format(datetime.timedelta(seconds=time.time() - start_time)))