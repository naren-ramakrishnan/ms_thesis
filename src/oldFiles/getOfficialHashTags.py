#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import division

__author__ = "Aravindan Mahendiran"
__email__ = "aravind@vt.edu"
__processor__ = "getOfficialHashTags"
__version__ = "1.0.0"

'''
Script to get an official list of hashtags
used by the candidates in during a time window
'''

import sys
import os
from etool import logs, args
import operator
import json
import re
from datetime import datetime
path = "/home/aravindan/Dropbox/git/geocoding/twitter"
if path not in sys.path:
    sys.path.insert(1, path)
from embers.geocode import Geo
from embers.utils import normalize_str

log = logs.getLogger(__processor__)

# building regex for URL
UTF_CHARS = ur'a-z0-9_\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff'
PRE_CHARS = ur'(?:[^/"\':!=]|^|\:)'
DOMAIN_CHARS = ur'([\.-]|[^\s_\!\.\/])+\.[a-z]{2,}(?::[0-9]+)?'
PATH_CHARS = ur'(?:[\.,]?[%s!\*\'\(\);:=\+\$/%s#\[\]\-_,~@])' % (UTF_CHARS, '%')
QUERY_CHARS = ur'[a-z0-9!\*\'\(\);:&=\+\$/%#\[\]\-_\.,~]'
PATH_ENDING_CHARS = r'[%s\)=#/]' % UTF_CHARS
QUERY_ENDING_CHARS = '[a-z0-9_&=#]'
URL_REGEX = '((%s)((https?://|www\\.)(%s)(\/%s*%s?)?(\?%s*%s)?))' % (PRE_CHARS, DOMAIN_CHARS, PATH_CHARS,
                                                                     PATH_ENDING_CHARS, QUERY_CHARS, QUERY_ENDING_CHARS)


def extract_hash_tags(string):
    hashTags = re.findall(r'#\w+', string)
    return list(set(hashTags))


def getCandidate(userId, realName, uids):
    candidate = None
    for cand in uids:
        if userId in uids[cand] or userId[1:] in uids[cand] or realName in uids[cand]:
            candidate = cand
    return candidate


def execute(arg):
    logs.init(arg)

    fromDate = datetime.strptime(arg.fromDate, "%d %b %Y")
    toDate = datetime.strptime(arg.toDate, "%d %b %Y")
    tweetFolder = arg.tweetFolder
    country = arg.country

    hashTagCounts = {}
    uids = {}

    # loading twitter handles from a file
    with open(arg.seedFile, 'r') as _file:
        for line in _file:
            handle, candidate = line.strip().split(',')
            if candidate not in uids:
                uids[candidate] = []
                hashTagCounts[candidate] = {}
                uids[candidate].append(handle.lower())
            else:
                uids[candidate].append(handle.lower())

    # for geolocation
    geo = Geo()

    for _file in sorted(os.listdir(tweetFolder)):
        fileDate = datetime.strptime(_file[17:27], '%Y-%m-%d')
        if (fileDate >= fromDate and fileDate < toDate):
            log.info("processing file %s" % (_file))
            try:
                with open(tweetFolder + "/" + _file, "r") as FILE:
                    for line in FILE:
                        try:
                            jsonTweet = json.loads(line.strip())
                            dateStr = jsonTweet['interaction']['created_at'][5:16]
                            tweetDate = datetime.strptime(dateStr, '%d %b %Y')
                            geoList = geo.geo_normalize(jsonTweet)
                            city, ctry, state = geoList[:3]
                            if ctry and (ctry.lower() == country) and (tweetDate >= fromDate) and (tweetDate <= toDate):
                                userId, realName = None, None
                                if 'twiiter' in jsonTweet:
                                    if 'user' in jsonTweet['twitter']:
                                        if 'screen_name' in jsonTweet['twitter']['user']:
                                            userId = jsonTweet['twitter']['user']['screen_name'].lower()
                                        if 'name' in jsonTweet['twitter']['user']:
                                            realName = jsonTweet['twitter']['user']['name'].lower()
                                if userId is None and realName is None:
                                    continue
                                log.debug('userId or realName is not None')
                                candidate = getCandidate(userId, realName, uids)
                                if candidate is not None:
                                    log.debug('found candidate--> ' + candidate)
                                    # prereProcess the tweet
                                    text = jsonTweet["interaction"]["content"]
                                    text = re.sub(URL_REGEX, ' ', text)  # remove urls
                                    text = re.sub('[^A-Za-z_@#0-9]', ' ', normalize_str(text, lower=True))  # allow only alphaNumerics and twitter tags
                                    text = re.sub(' +', ' ', text)  # remove multiple spaces
                                    hashTags = extract_hash_tags(text)
                                    hashTags = [hashTag for hashTag in hashTags if len(hashTag) > 3]
                                    for hashTag in hashTags:
                                        if hashTag.startswith('#'):
                                            hashTag = hashTag[1:]
                                        if hashTag in hashTagCounts[candidate]:
                                            hashTagCounts[candidate][hashTag] += 1
                                        else:
                                            hashTagCounts[candidate][hashTag] = 1
                        except Exception, e:
                            log.exception('error processing tweet %s' % e)
            except Exception, f:
                log.exception('error processing file %s' % f)
        else:
            log.debug('skipping file %s ' % _file)

    log.info('all tweets processed')

    for candidate in hashTagCounts:
        max = 0
        for hashTag in hashTagCounts[candidate]:
            if hashTagCounts[candidate][hashTag] > max:
                max = hashTagCounts[candidate][hashTag]
        for hashTag in hashTagCounts[candidate]:
            hashTagCounts[candidate][hashTag] = hashTagCounts[candidate][hashTag] / max

    for candidate in hashTagCounts:
        with open(arg.dataFolder + '/' + candidate + '_officialHashTags.csv', 'w') as file:
            sorted_tuples = sorted(hashTagCounts[candidate].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (hashTag, weight) in sorted_tuples:
                file.write(hashTag + ',' + str(weight))
                file.write('\n')


if __name__ == "__main__":
    ap = args.get_parser()
    ap.add_argument('-t', '--tweetFolder', type=str,
                    help='inputFolder pointing to PSLs output',
                    default='/hdd/tweets/2012/oct')
    ap.add_argument('-df', '--dataFolder', type=str,
                    help='folder to store intermediate outputs and final outputs',
                    default='/home/aravindan/Dropbox/git/ms_thesis/data/psl')
    ap.add_argument('-s', '--seedFile', type=str,
                    help='seed File containing the official handles of candidates',
                    default='/home/aravindan/Dropbox/git/ms_thesis/psl/seedWords/venezuela.csv')
    ap.add_argument('-c', '--country', type=str,
                    help='country to model elections for',
                    default='venezuela')
    ap.add_argument('-d1', '--fromDate', type=str,
                    help='date from which to track tweets',
                    default='01 Oct 2012')
    ap.add_argument('-d2', '--toDate', type=str,
                    help='date to which to track tweets',
                    default='06 Oct 2012')
    arg = ap.parse_args()
    execute(arg)
