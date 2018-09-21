#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Copyright (c) 2018 CA.  All rights reserved.
Created on Thu Jul 19 15:02:05 2018
@author: Vaclav Koudelka
@email: vaclav.koudelka@ca.com
LogDownloader is helper class for storing log files in local file system. Each
log file is saved in separate file and records are delimited by DELIMETER constant.
'''


'''Custom delimeter inserted during file download.
It is used to separate particular log records in downloaded binary files'''
DELIMETER = bytes((0xb2, 0x1d, 0x2d))

'''My distinction between particular logs is based on \r\n separator 
return from text file download. These separators are not in the binary downloads
however there can be combination of bytes which are interpreted the same way 
as \r\n. I need to identify these so I don't split records in the middle'''
MVSDELIMETER = (bytes((0x0D, 0x15)),bytes((0x0D, 0x25)))

class LogDownloader:
    ''' Mainframe download helper class
    Enables to track record length of variable length sequential datasets.
    '''
    
    def  __init__(self, fd, lrecs):
        assert (len(lrecs) > 0)
        self.fd = fd                          # Opened file for writing output
        self.lrecs = lrecs                    # list with record lengths
        self.current_position = 0             # current position in buffer
        self.bytes_to_record_end = lrecs[0]   # Bytes left to write to output record 
        self.bytes_flushed = 0                # Bytes flushed after error
    
    def done(self):
        ''' Healthcheck information. Flushed bytes = 0 indicates all went well. '''
        print('Download finished. Flushed bytes: {}'.format(self.bytes_flushed))
    
    def get_next_lrec(self):
        self.current_position += 1
        if (self.current_position >= len(self.lrecs)): return -1
        return self.lrecs[self.current_position]
    
    def prolong_next_record(self):
        self.lrecs[self.current_position+1] += 2
    
    def write(self, block):
        if (self.bytes_to_record_end == -1):
            self.bytes_flushed += len(block)
            return
        if (self.bytes_to_record_end > len(block)):
            # whole block is fitting to output record
            self.fd.write(block)
            self.bytes_to_record_end -= len(block)
            if __debug__: print('writing whole block '+str(len(block)))
        else:
            # block needs to be splitted with delimeter
            bytes_written = 0
            while (self.bytes_to_record_end < (len(block) - bytes_written)):
                # write what you can and terminate it with delimeter
                self.fd.write(block[bytes_written:(bytes_written + self.bytes_to_record_end)])
                bytes_written += self.bytes_to_record_end
                if not next_bytes_delimeter(block[bytes_written:]):
                    self.fd.write(DELIMETER)
                else:
                    # fake delimeter in file -> next record must be 2 bytes longer
                    self.prolong_next_record()                
                if __debug__: 
                    print('writing piece of block {}, left to write {}'.format(self.bytes_to_record_end, 
                                                                               (len(block) - bytes_written)))                
                self.bytes_to_record_end = self.get_next_lrec()
                if (self.bytes_to_record_end == -1): 
                    self.bytes_flushed += (len(block) - bytes_written)
                    break                

            # write the rest                
            self.fd.write(block[bytes_written:])
            self.bytes_to_record_end -= (len(block) - bytes_written)
            if __debug__: print('writing rest '+str(len(block) - bytes_written))

def next_bytes_delimeter(ba):
    if (len(ba) < 2): return False   # todo could be in next block
    for d in MVSDELIMETER:
        if (ba[:2] == d): return True
    return False
    
        
        