#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import division

__author__ = "Aravindan Mahendiran"
__email__ = "aravind@vt.edu"
__processor__ = "DQE_BASELINE"
__version__ = "1.0.0"

'''
Baseline DQE script
Filters tweets from based on previous run's rankings
counts the hashtags
ranks them according to the counts
uses top words for next round of filtering
'''

import sys
import os
from etool import logs, args
import operator
import json
import re
from datetime import datetime, timedelta
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


def trackTweets(tweetFolder, vocab, fromDate, toDate, country, threshold):
    counts = {}
    regex = {}
    totalWords = 0
    # building regex for each group
    for group in vocab:
        counts[group] = {}
        sorted_tuples = sorted(vocab[group].iteritems(), key=operator.itemgetter(1), reverse=True)
        words = []
        if len(sorted_tuples) <= 20:
            threshold = len(sorted_tuples)
        else:
            threshold = int(len(sorted_tuples) * threshold // 100)
        for (word, weight) in sorted_tuples[:threshold]:
            words.append(word)
            totalWords += 1
        regex[group] = re.compile(r'\b%s\b' % '\\b|\\b'.join(words), flags=re.IGNORECASE)

    log.info("tracking total of %d words" % totalWords)
    # for geoCoding tweets
    geo = Geo()

    tweetCount, tweetErrorCount = 0, 0
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
                                # prereProcess the tweet
                                text = jsonTweet["interaction"]["content"]
                                text = re.sub(URL_REGEX, ' ', text)  # remove urls
                                text = re.sub('[^A-Za-z_@#0-9]', ' ', normalize_str(text, lower=True))  # allow only alphaNumerics and twitter tags
                                text = re.sub(' +', ' ', text)  # remove multiple spaces
                                for group in regex:
                                    keywordsPresent = re.findall(regex[group], text)
                                    if len(keywordsPresent) > 0:
                                        keywordsPresent = list(set(keywordsPresent))
                                        hashTags = extract_hash_tags(text)
                                        hashTags = [hashTag for hashTag in hashTags if len(hashTag) > 3]
                                        hashTags.extend(keywordsPresent)
                                        for hashTag in hashTags:
                                            if hashTag.startswith('#'):
                                                hashTag = hashTag[1:]
                                            if hashTag in counts[group]:
                                                counts[group][hashTag] += 1
                                            else:
                                                counts[group][hashTag] = 1
                                        tweetCount += 1
                        except Exception, f:
                            log.debug("error processing tweet %s", f)
                            tweetErrorCount += 1
            except Exception, e:
                log.exception("error processfing file %s", e)
        else:
            log.debug("skipping file %s" % (_file))

    log.info("tweets used: %s" % str(tweetCount))
    log.debug("tweetErrorCount : %s" % str(tweetErrorCount))
    return counts


def main():
    ap = args.get_parser()
    ap.add_argument('-t', '--tweetFolder', type=str,
                    help='inputFolder pointing to PSLs output',
                    default='/hdd/tweets/2012/oct')
    ap.add_argument('-df', '--dataFolder', type=str,
                    help='folder to store intermediate outputs and final outputs',
                    default='/home/aravindan/Dropbox/git/ms_thesis/data/dqe')
    ap.add_argument('-wt', '--wordThreshold', type=float,
                    help='n-percent of words to propagate to next iteration',
                    default=25)
    ap.add_argument('-s', '--seedFile', type=str,
                    help='seed File containing the intial seed vocabulary',
                    default='/home/aravindan/Dropbox/git/ms_thesis/psl/seedWords/venezuela.csv')
    ap.add_argument('-d1', '--fromDate', type=str,
                    help='date from which to track tweets',
                    default='01 Oct 2012')
    ap.add_argument('-d2', '--toDate', type=str,
                    help='date to which to track tweets',
                    default='06 Oct 2012')
    ap.add_argument('-w', '--window', type=int,
                    help='number of days of tweets used to infer',
                    default=1)
    ap.add_argument('-c', '--country', type=str,
                    help='country to execute the pipeline for',
                    default='venezuela')

    arg = ap.parse_args()
    logs.init(arg)

    log.info("*************************************")
    log.info("PSL 4 Elections pipeline initializing")
    log.info("tweet folder------> " + arg.tweetFolder)
    log.info("dataFolder--------> " + str(arg.dataFolder))
    log.info("fromDate----------> " + arg.fromDate)
    log.info("toDate------------> " + arg.toDate)
    log.info("window------------> " + str(arg.window) + " day(s)")
    log.info("country-----------> " + arg.country)
    log.info("wordThreshold-----> " + str(arg.wordThreshold) + "%")
    log.info("*************************************")

    fromDate = datetime.strptime(arg.fromDate, "%d %b %Y")
    toDate = datetime.strptime(arg.toDate, "%d %b %Y")
    currentDate = fromDate

    iterCount = 1
    vocab = {}

    while(currentDate <= toDate):
        log.info("iterCount--------------->" + str(iterCount))
        log.info("processing PSL pipeline for %s" % (currentDate.strftime("%d %b %Y")))

        log.info("creating the directory substructure for current date")
        outputFolder = arg.dataFolder + '/' + arg.country + '/' + currentDate.strftime("%d%b")
        os.system('mkdir -p ' + outputFolder)

        nextDate = currentDate + timedelta(days=arg.window)

        if iterCount == 1:
            with open(arg.seedFile, 'r') as file:
                for line in file:
                    word, group, weight = line.split(',')
                    if group in vocab:
                        vocab[group][word] = weight
                    else:
                        vocab[group] = {}
                        vocab[group][word] = weight

        counts = trackTweets(arg.tweetFolder, vocab, currentDate, nextDate, arg.country, arg.wordThreshold)
        log.info("***********trackTweets complete***************")

        # normalize the counts for each group
        for group in counts:
            max = 0
            for word in counts[group]:
                if counts[group][word] > max:
                    max = counts[group][word]
            for word in counts[group]:
                weight = counts[group][word] / max
                vocab[group][word] = weight

        # dumping the vocab learnt
        for group in vocab:
            with open(outputFolder + '/' + group + '_vocab.csv', 'w') as f:
                sorted_tuples = sorted(vocab[group].iteritems(), key=operator.itemgetter(1), reverse=True)
                for (word, weight) in sorted_tuples:
                    f.write(word + ',' + str(weight))
                    f.write('\n')

        currentDate = nextDate
        iterCount += 1
        log.info("*********************************************")

    log.info("************ALL iterations complete********************")

if __name__ == "__main__":
    main()
