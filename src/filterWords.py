#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
__author__ = "Aravindan Mahendiran"
__email__ = "aravind@vt.edu"
__processor__ = "PSL4ElectionsPipeline"
__version__ = "3.3.0"

'''
Script to pipeline the entire PSL process for
election modeling.
tracks hashTags only
each window is a separate iteration
preprocess uses tweets that match the keyword filter
mentions and retweets are encoded as two separate predicates
likes and hates are encoded as belongsTo and ~belongsTo
'''

import os
from datetime import datetime
from etool import args


def execute(arg):
    dataFolder = arg.dataFolder + '/' + arg.country
    folders = os.listdir(dataFolder)
    folders = sorted(folders, key=lambda x: datetime.strptime(x, '%d%b'))
    timeSeries = {}

    for dateStr in folders:
        inputFolder = dataFolder + '/' + dateStr + '/inputs'
        belongedToFile = inputFolder + '/belongedTo.csv'
        print belongedToFile
        try:
            with open(belongedToFile, 'r') as _file:
                for line in _file:
                    word, group, weight = line.strip().split(',')
                    if group not in timeSeries:
                        timeSeries[group] = {}
                    if word not in timeSeries[group]:
                        timeSeries[group][word] = {}
                        timeSeries[group][word][dateStr] = weight
                    else:
                        timeSeries[group][word][dateStr] = weight
        except:
            print "skipping"

    #print timeSeries
    for group in timeSeries:
        with open(dataFolder + '/' + group + '_timeSeries.csv', 'w') as f:
            firstLine = 'word'
            for dateStr in folders:
                firstLine = firstLine + ',' + dateStr
            f.write(firstLine)
            f.write('\n')

            for word in sorted(timeSeries[group], key=lambda key: timeSeries[group][key]):
                line = word
                for dateStr in folders:
                    if dateStr in timeSeries[group][word]:
                        line = line + ',' + timeSeries[group][word][dateStr]
                    else:
                        line = line + ',' + '-'
                f.write(line)
                f.write('\n')


if __name__ == "__main__":
    ap = args.get_parser()
    ap.add_argument('-df', '--dataFolder', type=str,
                    help='folder to store intermediate outputs and final outputs',
                    default='/home/aravindan/Dropbox/git/ms_thesis/data/psl')
    ap.add_argument('-ut', '--userThreshold', type=float,
                    help='probability threshold of user membership',
                    default=0.60)
    ap.add_argument('-wt', '--wordThreshold', type=float,
                    help='probability threshold for vocab',
                    default=0.70)
    ap.add_argument('-c', '--country', type=str,
                    help='country to model elections for',
                    default='venezuela')
    arg = ap.parse_args()

    execute(arg)
