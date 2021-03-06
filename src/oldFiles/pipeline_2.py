#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
__author__ = "Aravindan Mahendiran"
__email__ = "aravind@vt.edu"
__processor__ = "PSL4ElectionsPipeline"
__version__ = "1.0.0"

'''
Script to pipeline the entire PSL process for
election modeling.
mentions and replies are encoded as same predicate
each file is an iteration
'''

import sys
import os
from etool import logs, args
import operator
import json
import re
from datetime import datetime  # , timedelta
path = "/home/aravindan/Dropbox/git/geocoding/twitter"
if path not in sys.path:
    sys.path.insert(1, path)
from embers.geocode import Geo
from embers.utils import normalize_str
from nltk.corpus import stopwords
# wordcloud imports
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont
import numpy as np
from query_integral_image import query_integral_image
import random

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

# fontPath for wordcloud
FONT_PATH = '/usr/share/fonts/truetype/ubuntu-font-family/UbuntuMono-R.ttf'


def make_wordcloud(words, counts, fname, font_path=None, width=1000, height=500,
                   margin=5, ranks_only=False):
    """Build word cloud using word counts, store in image."""

    if len(counts) <= 0:
        print("We need at least 1 word to plot a word cloud, got %d."
              % len(counts))

    if font_path is None:
        font_path = FONT_PATH

    if not os.path.exists(font_path):
        raise ValueError("The provided font %s does not exist." % font_path)

    # normalize counts
    counts = counts / float(counts.max())
    # sort words by counts
    inds = np.argsort(counts)[::-1]
    counts = counts[inds]
    words = words[inds]
    # create image
    img_grey = Image.new("L", (width, height))
    draw = ImageDraw.Draw(img_grey)
    integral = np.zeros((height, width), dtype=np.uint32)
    img_array = np.asarray(img_grey)
    font_sizes, positions, orientations = [], [], []
    # intitiallize font size "large enough"
    font_size = 1000
    # start drawing grey image
    for word, count in zip(words, counts):
        # alternative way to set the font size
        if not ranks_only:
            font_size = min(font_size, int(100 * np.log(count + 100)))
        while True:
            # try to find a position
            font = ImageFont.truetype(font_path, font_size)
            # transpose font optionally
            orientation = random.choice([None, Image.ROTATE_90])
            transposed_font = ImageFont.TransposedFont(font,
                                                       orientation=orientation)
            draw.setfont(transposed_font)
            # get size of resulting text
            box_size = draw.textsize(word)
            # find possible places using integral image:
            result = query_integral_image(integral, box_size[1] + margin,
                                          box_size[0] + margin)
            if result is not None or font_size == 0:
                break
            # if we didn't find a place, make font smaller
            font_size -= 1

        if font_size == 0:
            # we were unable to draw any more
            break

        x, y = np.array(result) + margin // 2
        # actually draw the text
        draw.text((y, x), word, fill="white")
        positions.append((x, y))
        orientations.append(orientation)
        font_sizes.append(font_size)
        # recompute integral image
        img_array = np.asarray(img_grey)
        # recompute bottom right
        # the order of the cumsum's is important for speed ?!
        partial_integral = np.cumsum(np.cumsum(img_array[x:, y:], axis=1),
                                     axis=0)
        # paste recomputed part into old image
        # if x or y is zero it is a bit annoying
        if x > 0:
            if y > 0:
                partial_integral += (integral[x - 1, y:]
                                     - integral[x - 1, y - 1])
            else:
                partial_integral += integral[x - 1, y:]
        if y > 0:
            partial_integral += integral[x:, y - 1][:, np.newaxis]

        integral[x:, y:] = partial_integral

    # redraw in color
    img = Image.new("RGB", (width, height))
    draw = ImageDraw.Draw(img)
    everything = zip(words, font_sizes, positions, orientations)
    for word, font_size, position, orientation in everything:
        font = ImageFont.truetype(font_path, font_size)
        # transpose font optionally
        transposed_font = ImageFont.TransposedFont(font,
                                                   orientation=orientation)
        draw.setfont(transposed_font)
        #draw.text((position[1], position[0]), word, fill="hsl(%d" % random.randint(0, 255) + ", 80%, 50%)")
        draw.text((position[1], position[0]), word)
    #img.show()
    img.save(fname)


def getSentiment(jsonTweet):
    try:
        sentiment = jsonTweet["salience"]["content"]["sentiment"]
    except Exception, e:
        sentiment = 0
        log.debug("error getting sentiment:%s, setting sentiment=0", str(e))
    return sentiment


def getInteractions(jsonTweet):
    """method to find all mentions of a another user in a tweet"""
    mentions = []
    retweets = []
    try:
        if "retweeted" in jsonTweet["twitter"]:
            retweets.append(str(jsonTweet["twitter"]["retweeted"]["user"]["id"]))
    except Exception, e:
        log.debug("error getting retweet %s", str(e))
    try:
        if "mention_ids" in jsonTweet["twitter"]:
            for id in jsonTweet["twitter"]["mention_ids"]:
                mentions.append(str(id))
    except Exception, e:
        log.debug("error getting mentions %s", str(e))
    try:
        if "in_reply_to_user_id" in jsonTweet["twitter"]:
            mentions.append(jsonTweet["twitter"]["in_reply_to_user_id"])
    except Exception, e:
        log.debug("error getting replyTo %s ", str(e))
    mentions = list(set(mentions))
    return mentions, retweets


def postProcess(inputFolder, outputFolder, statsFolder, userThreshold, wordThreshold, membership, vocab, seedWordList, currentDate):
    log.debug("inside postProcess")
    memberFile = inputFolder + '/membership.tsv'
    belongsToFile = inputFolder + '/belongsTo.tsv'

    with open(belongsToFile, 'r') as file:
        for line in file:
            try:
                predicate, weight = line.split('\t')
                word, group = predicate.split(',')
                word = word[11:]
                group = group[1:-1]
                weight = (float)(weight.strip('\n'))

                if weight > 1.0:
                    weight = 1.0

                if group in vocab:
                    vocab[group][word] = weight
                else:
                    vocab[group] = {}
                    vocab[group][word] = weight
            except Exception, e:
                log.exception('error while processing belongsTo %s', str(e))

    with open(memberFile, 'r') as file:
        for line in file:
            try:
                predicate, weight = line.split('\t')
                user, group = predicate.split(',')
                group = group[1:-1]
                user = user[10:]
                weight = (float)(weight.strip('\n'))

                if weight > 1.0:
                    weight = 1.0

                if group in membership:
                    membership[group][user] = weight
                else:
                    membership[group] = {}
                    membership[group][user] = weight
            except Exception, e:
                log.exception('error while processing memberFile %s', str(e))

    wasMemberFile = outputFolder + '/wasMember.csv'
    belongedToFile = outputFolder + '/belongedTo.csv'

    # writing the wasMember file for next iteration of PSL
    with open(wasMemberFile, 'w') as file:
        for group in membership:
            sorted_tuples = sorted(membership[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (user, weight) in sorted_tuples:
                if weight >= userThreshold:
                    file.write(user + ',' + group + ',' + str(weight))
                    file.write('\n')

    # writing the belongedToFile for next iteration of PSL
    with open(belongedToFile, 'w') as file:
        for group in vocab:
            sorted_tuples = sorted(vocab[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (word, weight) in sorted_tuples:
                if weight >= wordThreshold and word not in seedWordList:
                    file.write(word + ',' + group + ',' + str(weight))
                    file.write('\n')

    # writing all of belongsTo to individual files
    for group in vocab:
        with open(statsFolder + '/' + group + '_' + currentDate.strftime("%d%b") + '_belongsTo.csv', 'w') as file:
            sorted_tuples = sorted(vocab[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (word, weight) in sorted_tuples:
                if weight >= 0.50:
                    file.write(word + ',' + str(weight))
                    file.write('\n')

    # writing all of membership to individual files for tracking user membership growth
    for group in membership:
        with open(statsFolder + '/' + group + '_' + currentDate.strftime("%d%b") + '_supporters.csv', 'w') as file:
            sorted_tuples = sorted(membership[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (user, weight) in sorted_tuples:
                if weight >= 0.50:
                    file.write(user + ',' + str(weight))
                    file.write('\n')

    # belongsToWordCloud generation
    try:
        for group in vocab:
            wordsArray = []
            weightsArray = []
            fileName = statsFolder + '/' + group + '_belongsTo.jpeg'
            for word in vocab[group]:
                wordsArray.append(word)
                weightsArray.append(vocab[group][word])
            wordsArray = np.array(wordsArray)
            weightsArray = np.array(weightsArray)
            make_wordcloud(wordsArray, weightsArray, fileName)
    except Exception, e:
        log.exception("ERROR!! while creating belongsTo wordCloud %s", e)

    # updating keywordList for next preProcess
    newKeyWordList = []
    for group in vocab:
        for word in vocab[group]:
            if vocab[group][word] >= wordThreshold:
                newKeyWordList.append(word)

    return newKeyWordList


def preProcess(tweetFolder, outputFolder, keywordList, fromDate, toDate, country, filesProcessed):
    log.info("inside preProcess")
    log.debug("fromDate-->" + fromDate.strftime("%d %b %Y"))
    log.debug("toDate-->" + toDate.strftime("%d %b %Y"))

    tweetCount, tweetErrorCount = 0, 0
    tweets = {}

    # output files
    tweetedFile = open(outputFolder + '/tweeted.csv', 'w')
    mentionFile = open(outputFolder + '/mentioned.csv', 'w')
    retweetFile = open(outputFolder + '/retweet.csv', 'w')
    wordsFile = open(outputFolder + '/containsWord.csv', 'w')
    sentimentFile = open(outputFolder + '/sentiment.csv', 'w')
    tweetsFile = open(outputFolder + '/tweets.json', 'w')

    # build stop word list
    englishStopWords = [normalize_str(w).lower() for w in stopwords.words('english')]
    spanishStopWords = [normalize_str(w).lower() for w in stopwords.words('spanish')]
    stopWordList = []
    stopWordList.extend(englishStopWords)
    stopWordList.extend(spanishStopWords)

    log.info("# of keywords: " + str(len(keywordList)))
    log.info("tracking--> " + str(keywordList))
    # build regular expression for keyword
    keywordRegex = re.compile(r'\b%s\b' % '\\b|\\b'.join(keywordList),
                              flags=re.IGNORECASE)

    # for geocoding tweets
    geo = Geo()

    log.info("filesProcessed-->" + str(filesProcessed))
    for _file in sorted(os.listdir(tweetFolder)):
        fileDate = datetime.strptime(_file[17:27], '%Y-%m-%d')

        if (_file not in filesProcessed and fileDate >= fromDate and fileDate < toDate):
            log.info("processing file %s" % (_file))
            try:
                with open(tweetFolder + "/" + _file, "r") as FILE:
                    tweetCount, tweetErrorCount = 0, 0
                    for line in FILE:
                        try:
                            jsonTweet = json.loads(line.strip())
                            dateStr = jsonTweet['interaction']['created_at'][5:16]
                            tweetDate = datetime.strptime(dateStr, '%d %b %Y')
                            sentiment = getSentiment(jsonTweet)
                            if sentiment == 0:
                                continue
                            geoList = geo.geo_normalize(jsonTweet)
                            ctry, a1, a2, a3 = geoList[1:5]
                            if ctry and (ctry.lower() == country) and (tweetDate >= fromDate) and (tweetDate <= toDate):
                                text = jsonTweet["interaction"]["content"]
                                text = re.sub(URL_REGEX, ' ', text)  # remove urls
                                text = re.sub('[^A-Za-z_@#0-9]', ' ', normalize_str(text, lower=True))  # allow only alphaNumerics and twitter tags
                                text = re.sub(' +', ' ', text)  # remove multiple spaces

                                keywordsPresent = keywordRegex.search(text)
                                if keywordsPresent is not None:
                                    words = text.split(" ")
                                    words = [w for w in words if len(w) > 2 and w not in stopWordList]
                                    words2 = []
                                    for word in words:
                                        for w in word:
                                            if (word not in keywordList) and (w.isdigit() or w == '@'):
                                                break
                                        else:
                                            if word[0] == '#':
                                                word = word[1:]
                                            words2.append(word)

                                    tweetId = jsonTweet["twitter"]["id"]
                                    tweeterId = str(jsonTweet["interaction"]["author"]["id"])
                                    mentions, retweets = getInteractions(jsonTweet)

                                    tweetedFile.write(tweeterId + ',' + tweetId + '\n')
                                    sentimentFile.write(tweetId + ',' + str(sentiment) + '\n')
                                    for userId in mentions:
                                        mentionFile.write(tweetId + ',' + userId + '\n')
                                    for userId in retweets:
                                        retweetFile.write(tweetId + ',' + userId + '\n')
                                    for word in words2:
                                        wordsFile.write(tweetId + ',' + word + '\n')
                                    # tracking the tweets for checks.
                                    if tweeterId in tweets:
                                        tweets[tweeterId][tweetId] = jsonTweet["interaction"]["content"]
                                    else:
                                        tweets[tweeterId] = {}
                                        tweets[tweeterId][tweetId] = jsonTweet["interaction"]["content"]

                                    tweetCount += 1
                        except Exception, f:
                            log.exception("error processing tweet %s", f)
                            tweetErrorCount += 1
            except Exception, e:
                log.exception("error processfing file %s", e)
            log.info("tweets used: %s" % str(tweetCount))
            log.debug("tweetErrorCount : %s" % str(tweetErrorCount))
            filesProcessed.append(_file)
            break
        else:
            log.debug("skipping file %s" % (_file))

    tweetsFile.write(json.dumps(tweets, indent=4))
    tweetsFile.close()
    mentionFile.close()
    retweetFile.close()
    wordsFile.close()
    tweetedFile.close()
    sentimentFile.close()

    return fileDate, filesProcessed


def executePSLCode(inputFolder, outputFolder, classPathFile):
    commandToExecute = "java -Xmx12g -cp /home/aravindan/Dropbox/git/ms_thesis/psl/ElectionsPSL/target/classes:`cat " + classPathFile + "` edu.umd.cs.linqs.embers.electionsPSL " + inputFolder + " " + outputFolder
    # log.info("\n\n" + commandToExecute + "\n\n")
    os.system(commandToExecute)
    return


def main():
    ap = args.get_parser()
    ap.add_argument('-t', '--tweetFolder', type=str,
                    help='inputFolder pointing to PSLs output',
                    default='/hdd/tweets/2012/oct')
    ap.add_argument('-df', '--dataFolder', type=str,
                    help='folder to store intermediate outputs and final outputs',
                    default='/home/aravindan/Dropbox/git/ms_thesis/data/psl')
    ap.add_argument('-ut', '--userThreshold', type=float,
                    help='probability threshold of user membership',
                    default=0.60)
    ap.add_argument('-wt', '--wordThreshold', type=float,
                    help='probability threshold for vocab',
                    default=0.70)
    ap.add_argument('-s', '--seedFile', type=str,
                    help='seed File containing the intial seed vocabulary',
                    default='/home/aravindan/Dropbox/git/ms_thesis/psl/seedWords/venezuela.csv')
    ap.add_argument('-c', '--country', type=str,
                    help='country to model elections for',
                    default='venezuela')
    ap.add_argument('-w', '--window', type=int,
                    help='number of days of tweets used to infer',
                    default=1)
    ap.add_argument('-d1', '--fromDate', type=str,
                    help='date from which to track tweets',
                    default='01 Oct 2012')
    ap.add_argument('-d2', '--toDate', type=str,
                    help='date to which to track tweets',
                    default='06 Oct 2012')
    ap.add_argument('-cp', '--classPathFile', type=str,
                    help='file containing class path for PSL execution',
                    default='/home/aravindan/Dropbox/git/ms_thesis/psl/classPathFile.txt')

    arg = ap.parse_args()
    logs.init(arg)

    log.info("*************************************")
    log.info("PSL 4 Elections pipeline initializing")
    log.info("tweet folder------> " + arg.tweetFolder)
    log.info("dataFolder--------> " + str(arg.dataFolder))
    log.info("fromDate----------> " + arg.fromDate)
    log.info("toDate------------> " + arg.toDate)
    log.info("window------------> " + str(arg.window) + " day(s)")
    log.info("userThreshold-----> " + str(arg.userThreshold))
    log.info("userThreshold-----> " + str(arg.userThreshold))
    log.info("*************************************")

    fromDate = datetime.strptime(arg.fromDate, "%d %b %Y")
    toDate = datetime.strptime(arg.toDate, "%d %b %Y")
    currentDate = fromDate

    iterCount = 1
    membership = {}
    vocab = {}
    filesProcessed = []

    while(currentDate <= toDate):
        log.info("iterCount--------------->" + str(iterCount))
        log.info("processing PSL pipeline for %s" % (currentDate.strftime("%d %b %Y")))

        log.info("creating the directory substructure for current iteration")
        inputFolder = arg.dataFolder + '/' + arg.country + '/iteration' + str(iterCount) + '/inputs'
        outputFolder = arg.dataFolder + '/' + arg.country + '/iteration' + str(iterCount) + '/outputs'
        statsFolder = arg.dataFolder + '/' + arg.country + '/iteration' + str(iterCount) + '/stats'
        os.system('mkdir -p ' + inputFolder)
        os.system('mkdir -p ' + outputFolder)
        os.system('mkdir -p ' + statsFolder)

        log.info("creating the directory substructure for next iteration")
        nextInputFolder = arg.dataFolder + '/' + arg.country + '/iteration' + str(iterCount + 1) + '/inputs'
        nextOutputFolder = arg.dataFolder + '/' + arg.country + '/iteration' + str(iterCount + 1) + '/outputs'
        nextStatsFolder = arg.dataFolder + '/' + arg.country + '/iteration' + str(iterCount + 1) + '/stats'
        os.system('mkdir -p ' + nextInputFolder)
        os.system('mkdir -p ' + nextOutputFolder)
        os.system('mkdir -p ' + nextStatsFolder)

        if iterCount == 1:
            keywordList = []
        # adding seed words to the list
        seedWordList = []
        with open(arg.seedFile, 'r') as file:
            for line in file:
                word, group, weight = line.split(',')
                seedWordList.append(word)
        keywordList = list(set(keywordList).union(set(seedWordList)))

        log.info("copying the seedFile to inputFolder")
        os.system('cp ' + arg.seedFile + ' ' + inputFolder + '/seedWords.csv')

        fileDate, filesProcessed = preProcess(arg.tweetFolder, inputFolder, keywordList, currentDate, toDate, arg.country, list(set(filesProcessed)))
        log.info("***********preProcess complete******************")

        executePSLCode(inputFolder, outputFolder, arg.classPathFile)
        log.info("***********PSL code complete********************")

        keywordList = postProcess(outputFolder, nextInputFolder, statsFolder, arg.userThreshold, arg.wordThreshold, membership, vocab, seedWordList, currentDate)
        log.info("**********postProcess complete*****************")

        if fileDate > currentDate:
            currentDate = fileDate

        log.info("deleting the database used for current iteration")
        os.system('rm /home/aravindan/Dropbox/git/ms_thesis/psl/electionPSLDB*')

        iterCount += 1
        log.info("**********************************************************")

if __name__ == "__main__":
    main()
