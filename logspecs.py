#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Copyright (c) 2018 CA.  All rights reserved.
Created on Wed Aug  1 15:54:49 2018
@author: Vaclav Koudelka
@email: vaclav.koudelka@ca.com
Script containing basic log dataframe manipulation functions.
It also contains descriptions of some log type records.
'''

import numpy as np
from enum import Flag, IntFlag
import datetime

def tod2datetime(tod):
    '''Converts mainframe 64bits TOD time to datetime'''
    if len(tod) != 16: return np.nan
    micros = int(tod[:13], 16)                 # microseconds since 1900
    epoch70_micros = int('7D91048BCA000', 16)  # microseconds since 1970
    micros_since_epoch = micros - epoch70_micros
    secs_since_epoch = micros_since_epoch / 1000000
    if secs_since_epoch > 0: return datetime.datetime.utcfromtimestamp(secs_since_epoch)
    else: return np.nan
    
def set_flag(flag, value):
    try: return flag(value)
    except ValueError: return np.nan


def extract_common_fields(df):
    '''Operates over dataframe with log records - from the binary blob
    it takes fields common for all types of IMS logs in the dataframe.
    Result is stored in the dataframe.'''
    df['type'] = df['blob'].apply(lambda x: x[0:1].hex())
    df['subtype'] = df['blob'].apply(lambda x: x[1:2].hex())
    df['sequence'] = df['blob'].apply(lambda x: int(x[-8:].hex(), 16))
    df['tod'] = df['blob'].apply(lambda x: int(x[-16:-8].hex(), 16))
    df['datetime'] = df['blob'].apply(lambda x: tod2datetime(x[-16:-8].hex()))
    
def add_descriptions(df):
    '''Adds log type descrion to records'''
    df['desc'] = df['type'].apply(lambda x: log_desc[x.upper()])


def extract_typespecific_fields(log_specifics, df):
    '''Operates over dataframe with log records - from the binary blob
    it takes specific fields defined by log_specific structure.
    Result is stored in the dataframe.'''
    for key,it in log_specifics.items():                        
        if it['type'] == 'txt': val = df['blob'].apply(lambda x: 
                                x[it['off_start']:it['off_end']].decode('cp500'))
        if it['type'] == 'int': val = df['blob'].apply(lambda x: 
                                int.from_bytes(x[it['off_start']:it['off_end']], byteorder='big'))
        if it['type'] == 'flg': val = df['blob'].apply(lambda x: 
                                set_flag(it['flags'], int.from_bytes(x[it['off_start']:it['off_start']+1], byteorder='big')))
        df[key] = val



'''Specific log records description follows'''
class log50DborgFlag(Flag):
    hdam  = 0x40
    hidam = 0x20
    dedb  = 0x10
    index = 0x8
    hisam = 0x4

class log50DsorgFlag(Flag):
    vsam  = 0x80
    osam  = 0x40
    esds  = 0x08
    ksds  = 0x04

class log50CallFlag(Flag):
    isrt  = 0x80
    repl  = 0x40
    dlet  = 0x20
    rolx  = 0x10

log_items = {
        '42': {'imsid': {'type': 'txt',  'off_start': 0x14,  'off_end': 0x18}},
        '4c': {'dbd':   {'type': 'txt',  'off_start': 0x4, 'off_end': 0xc}},
        '50': {'imsid': {'type': 'txt',  'off_start': 0x4,  'off_end': 0x8},
               'pgm':   {'type': 'txt',  'off_start': 0x28, 'off_end': 0x30},
               'dbd':   {'type': 'txt',  'off_start': 0x30, 'off_end': 0x38},
               'rba':   {'type': 'int',  'off_start': 0x3c, 'off_end': 0x40},
               'call':  {'type': 'flg',  'off_start': 0x3b, 'flags': log50CallFlag},
               'dborg': {'type': 'flg',  'off_start': 0x26, 'flags': log50DborgFlag},
               'dsorg': {'type': 'flg',  'off_start': 0x27, 'flags': log50DsorgFlag}},
        '67': {'ff': {'snid': {'type': 'txt', 'off_start': 0x4,  'off_end': 0x8},
                      'abno': {'type': 'txt', 'off_start': 0x18, 'off_end': 0x1c},
                      'name': {'type': 'txt', 'off_start': 0x1c, 'off_end': 0x24}}},
        '07': {'psb':    {'type': 'txt',  'off_start': 0x01, 'off_end': 0x09},
               'tran':   {'type': 'txt',  'off_start': 0x09, 'off_end': 0x11},
               'extime': {'type': 'int',  'off_start': 0x1c, 'off_end': 0x24},
               'ccode':  {'type': 'int',  'off_start': 0x2d, 'off_end': 0x30},
               'job':    {'type': 'txt',  'off_start': 0x30, 'off_end': 0x38},
               'step':   {'type': 'txt',  'off_start': 0x38, 'off_end': 0x40},
               'dlicnt': {'type': 'int',  'off_start': 0x68, 'off_end': 0x6c},
               'vsamrio':{'type': 'int',  'off_start': 0xfc, 'off_end': 0x100},
               'vsamwio':{'type': 'int',  'off_start': 0x100, 'off_end': 0x104},
               'osamrio':{'type': 'int',  'off_start': 0x104, 'off_end': 0x108},
               'osamwio':{'type': 'int',  'off_start': 0x108, 'off_end': 0x10c},
               'totio':  {'type': 'int',  'off_start': 0x10c, 'off_end': 0x110},
               'iotime': {'type': 'int',  'off_start': 0x194, 'off_end': 0x19c},
               'lktime': {'type': 'int',  'off_start': 0x19c, 'off_end': 0x1a4}
               }
        }

deadlock_map = {
        'dmb':          {'type': 'txt',  'off_start': 188,  'off_end': 196},
        'waiter_imsid': {'type': 'txt',  'off_start': 140,  'off_end': 148},
        'waiter_tran':  {'type': 'txt',  'off_start': 156,  'off_end': 164},
        'waiter_psb':   {'type': 'txt',  'off_start': 172,  'off_end': 180},
        'waiter_pcb':   {'type': 'txt',  'off_start': 180,  'off_end': 188},
        'waiter_pst':   {'type': 'int',  'off_start': 210,  'off_end': 212},
        'blockr_imsid': {'type': 'txt',  'off_start': 140+428,  'off_end': 148+428},
        'blockr_tran':  {'type': 'txt',  'off_start': 156+428,  'off_end': 164+428},
        'blockr_psb':   {'type': 'txt',  'off_start': 172+428,  'off_end': 180+428},
        'blockr_pcb':   {'type': 'txt',  'off_start': 180+428,  'off_end': 188+428},
        'blockr_pst':   {'type': 'int',  'off_start': 210+428,  'off_end': 212+428}
        }

log_desc = {
        '01': 'data put in mq buffer - DC',
        '02': '/LOG or command required for restart completed',
        '03': 'data put in mq buffer - DL/I',
        '04': 'relays information to RSR',
        '06': 'IMS started or stopped or FEOV issued',
        '07': 'app program terminated',
        '08': 'app programm scheduled',
        '09': 'potential user off sequential buffering terminated',
        '0A': 'CPI-communictaion driven app terminated or scheduled',
        '10': 'security violation occured',
        '11': 'conversational program started',
        '12': 'conversational program terminated',
        '13': 'logon for non-ISC or signon for ETO or ISC alloc',
        '14': 'dial line disconnected',
        '15': 'dial line connected',
        '16': '/SIGN command completed',
        '18': 'user program CHKP',
        '20': 'db opened',
        '21': 'db closed',
        '22': 'type-2 command completed',
        '23': 'listed dbs opened for batch app requesting a checkpoint',
        '24': 'buffer handler detected IO error',
        '25': 'EEQE created or deleted',
        '26': 'I/O toleration buffer created',
        '27': 'dataset extended',
        '28': 'IMS restart facility input messages seq. number update',
        '29': 'OLR progress',
        '30': 'message prefix changed',
        '31': 'GU issued for a message',
        '32': 'message rejected',
        '33': 'queue manager released a record',
        '34': 'message canceled',
        '35': 'message enqueued or re-enqueued',
        '36': 'message dequeued or saved or deleted',
        '37': 'records marked as NO INPUT and NO OUTPUT written',
        '38': 'input message put back on the input queue after app abnorm term',
        '39': 'output queue freed during cleanup processin of RELEASE call',
        '3A': 'bitmap record replaced after queue record was freed',
        '3B': 'invalid message record',
        '3C': 'control block change during DFSQFIX0 validation',
        '3D': 'QBLK altered during DFSQFIX0 processing',
        '40': 'checkpoint  taken',
        '41': 'batch app or BMP issued checkpoint',
        '42': 'OLDS switch',
        '43': 'log archive utility record',
        '45': 'checkpoint statistics gathered',
        '47': 'checkpoint just taken',
        '48': 'variable length padding record',
        '49': 'invalid FSE or FSEAP at the RSR tracking site',
        '4C': 'db activity (backout,write error,db/pgm started/stopped,...)',
        '4E': 'monitoring event',
        '50': 'db update',
        '52': 'ISRT for a new root in KSDS',
        '53': 'bitmap written for a log record for an alternate IMS',
        '55': 'external subsys information',
        '56': 'external subsys support recovery log record ID',
        '57': 'db updates in RSR',
        '59': 'FP related',
        '5E': 'sequential buffer image captured',
        '5F': 'DL/I call completed',
        '63': 'log session initiation/termination',
        '64': 'inconsistency associated with MSC',
        '65': 'message about to be enqueued',
        '66': 'message about to be enqueued or dequeued',
        '67': 'service trace record',
        '69': 'unauthorized 3275 terminal dialed in',
        '6C': 'MSC partner systems started',
        '6D': 'XRF related',
        '6E': 'SNA command processed',
        '70': 'online change /MODIFY sucessful',
        '72': 'dynamic terminal related',
        '99': 'created by EXIT= specified in DBDGEN'}