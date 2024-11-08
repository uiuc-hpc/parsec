#!/usr/bin/env python
from __future__ import print_function
import argparse
import struct
import re
from pathlib import Path
import numpy as np
import pandas as pd

def parsec_statfile(path, node, bfmt=False, interval=1000000):
    # coerce interval to int
    interval = int(interval)
    if bfmt:
        with path.open(mode='rb') as f:
            # first line is header
            header = f.readline().decode().strip().split(sep='\t')
            # second line is format string
            fmtstr = f.readline().decode().strip()
            # remainder is packed data
            data = {col : [] for col in header}
            for record in struct.iter_unpack(fmtstr, f.read()):
                for col, val in zip(header, record):
                    data[col].append(val)
        df = pd.DataFrame(data)
    else:
        df = pd.read_csv(path, sep='\t', header=0)
    # add Node column
    df['Node'] = node
    # compute 'virtual Time'
    df['vTime'] = (df['Time'] / interval).round() * interval
    # use vTime as index
    df.set_index('vTime', inplace=True)
    # remove duplicate entries, keep last value
    df = df[~df.index.duplicated(keep='last')]
    # add calculated Queue column
    df['Queue'] = df['Executed'] - df['Ready']
    return df

class ParsecStats(object):

    def __init__(self, basename, bfmt=False, interval=1000000):
        basepath = Path(basename).resolve()
        ext = 'bfmt' if bfmt else 'tsv'
        pattern = re.compile(r'{}-(\d+).{}'.format(basepath, ext))
        frames = [
            parsec_statfile(path, int(match.group(1)), bfmt, interval)
            for match, path in (
                (pattern.fullmatch(str(path)), path)
                for path in basepath.parent.iterdir()
            )
            if match
        ]
        # compute new indices
        idx_max = max(df.index.max() for df in frames)
        idx = pd.RangeIndex(0, idx_max + interval, interval, name='vTime')
        # reindex frames
        frames = [df.reindex(idx, method='ffill') for df in frames]

        # functions for aggregation
        agg_dict = {
            'Execution' : 'sum',
            'Select'    : 'sum',
            'Wait'      : 'sum',
            'Known'     : 'sum',
            'Ready'     : 'sum',
            'Executed'  : 'sum',
            'Completed' : 'sum',
            'Retired'   : 'sum',
            'Idle'      : 'sum',
            'Queue'     : 'sum',
        }

        # we don't always have SimTime or SimComm
        have_simtime = any('SimTime' in df.columns for df in frames)
        have_simcomm = any('SimComm' in df.columns for df in frames)
        if have_simtime:
            agg_dict['SimTime'] = 'max'
        if have_simcomm:
            agg_dict['SimComm'] = 'max'

        # concatenate and aggregate frames by vTime
        data = pd.concat(frames).groupby('vTime').agg(agg_dict)
        # determine number of cores
        cores = data['Idle'].max()
        # Missed Parallelism is minimum of queue length and idle cores
        data['MissedPar'] = data[['Queue', 'Idle']].min(axis=1)
        # Global Starvation is when there are idle cores
        # even if you could redistribute work
        data['Starvation'] = (data['Idle'] - data['Queue']).clip(lower=0)
        # percentage of time actively in computation
        data['Active%'] = (data['Execution'] / data['Wait']).fillna(0)

        if have_simtime:
            # commulative compute parallelism
            data['Par%'] = ((data['Execution'] / data['SimTime']) / cores).fillna(0)
            # how close computation was to critical path
            data['Crit%'] = (data['SimTime'] / (data['Wait'] / cores)).fillna(0)

        if have_simcomm:
            # how close computation was to critical path + communication
            data['CritComm%'] = (data['SimComm'] / (data['Wait'] / cores)).fillna(0)

        self.frames = frames
        self.data = data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description="Manipulate and export data from PaRSEC stats files")
    parser.add_argument('--binary-format', '--bfmt', dest='bfmt',
                        action='store_true', help='Files are in binary format')
    parser.add_argument('--interval', '--waitns', dest='interval',
                        action='store', help='Stat collection interval')
    parser.add_argument('basename', dest='basename',
                        help='Stats file base name')
    args = parser.parse_args()

