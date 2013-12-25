#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
__author__ = "Aravindan Mahendiran"
__email__ = "aravind@vt.edu"
__processor__ = "electionsPslPipeline"
__version__ = "0.0.0"

'''
Script to pipeline the entire PSL process for
election modeling.
seed words are encoded as separate predicate
tracks hashTags only
each window is a separate iteration
preprocess uses all tweets
mentions and retweets are encoded as mentions
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
#from wordcloud import make_wordcloud
#from nltk.corpus import stopwords

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


def getSentiment(jsonTweet):
    try:
        sentiment = jsonTweet["salience"]["content"]["sentiment"]
    except Exception:
        sentiment = 0
    return sentiment


def extract_hash_tags(string):
    hashTags = re.findall(r'#\w+', string)
    return list(set(hashTags))


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

    mentions = list(set(mentions).union(set(retweets)))

    return mentions


def postProcess(inputFolder, outputFolder, statsFolder, userThreshold, wordThreshold, membership, likes, hates):
    log.debug("inside postProcess")
    memberFile = inputFolder + '/membership.tsv'
    likesFile = inputFolder + '/likes.tsv'
    hatesFile = inputFolder + '/hates.tsv'

    with open(likesFile, 'r') as file:
        for line in file:
            try:
                predicate, weight = line.split('\t')
                word, group = predicate.split(',')
                word = word[6:]
                group = group[1:-1]
                weight = (float)(weight.strip('\n'))
                if weight > 1.0:
                    weight = 1.0
                if group in likes:
                    likes[group][word] = weight
                else:
                    likes[group] = {}
                    likes[group][word] = weight
            except Exception, e:
                log.exception('error while processing likesFile %s', str(e))

    with open(hatesFile, 'r') as file:
        for line in file:
            try:
                predicate, weight = line.split('\t')
                word, group = predicate.split(',')
                word = word[6:]
                group = group[1:-1]
                weight = (float)(weight.strip('\n'))
                if weight > 1.0:
                    weight = 1.0
                if group in hates:
                    hates[group][word] = weight
                else:
                    hates[group] = {}
                    hates[group][word] = weight
            except Exception, e:
                log.exception('error while processing hatesFile %s', str(e))

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
    likedFile = outputFolder + '/liked.csv'
    hatedFile = outputFolder + '/hated.csv'

    # writing the wasMember file for next iteration of PSL
    with open(wasMemberFile, 'w') as file:
        for group in membership:
            sorted_tuples = sorted(membership[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (user, weight) in sorted_tuples:
                if weight >= userThreshold:
                    file.write(user + ',' + group + ',' + str(weight))
                    file.write('\n')

    # writing the liked for next iteration of PSL
    with open(likedFile, 'w') as file:
        for group in likes:
            sorted_tuples = sorted(likes[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (word, weight) in sorted_tuples:
                if weight >= wordThreshold:
                    file.write(word + ',' + group + ',' + str(weight))
                    file.write('\n')

    # writing the hated for next iteration of PSL
    with open(hatedFile, 'w') as file:
        for group in hates:
            sorted_tuples = sorted(hates[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (word, weight) in sorted_tuples:
                if weight >= wordThreshold:
                    file.write(word + ',' + group + ',' + str(weight))
                    file.write('\n')

    # writing all of likes to individual files
    for group in likes:
        with open(statsFolder + '/' + group + '_likes.csv', 'w') as file:
            sorted_tuples = sorted(likes[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (word, weight) in sorted_tuples:
                if weight >= wordThreshold:
                    file.write(word + ',' + str(weight))
                    file.write('\n')

    # writing all of hates to individual files
    for group in hates:
        with open(statsFolder + '/' + group + '_hates.csv', 'w') as file:
            sorted_tuples = sorted(hates[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (word, weight) in sorted_tuples:
                if weight >= wordThreshold:
                    file.write(word + ',' + str(weight))
                    file.write('\n')

    # writing all of membership to individual files for tracking user membership growth
    for group in membership:
        with open(statsFolder + '/' + group + '_supporters.csv', 'w') as file:
            sorted_tuples = sorted(membership[group].iteritems(), key=operator.itemgetter(1), reverse=True)
            for (user, weight) in sorted_tuples:
                if weight >= userThreshold:
                    file.write(user + ',' + str(weight))
                    file.write('\n')
    ## updating keywordList for next preProcess
    #newKeyWordList = []
    #for group in likes:
    #    for word in likes[group]:
    #        if likes[group][word] >= wordThreshold:
    #            newKeyWordList.append(word)
    #for group in hates:
    #    for word in hates[group]:
    #        if hates[group][word] >= wordThreshold:
    #            newKeyWordList.append(word)
    #
    #return newKeyWordList
    return


def preProcess(tweetFolder, outputFolder, keywordList, fromDate, toDate, country):
    log.info("inside preProcess")
    log.debug("fromDate-->" + fromDate.strftime("%d %b %Y"))
    log.debug("toDate-->" + toDate.strftime("%d %b %Y"))

    tweets = {}

    # output files
    tweetedFile = open(outputFolder + '/tweeted.csv', 'w')
    mentionFile = open(outputFolder + '/mentioned.csv', 'w')
    # retweetFile = open(outputFolder + '/retweet.csv', 'w')
    wordsFile = open(outputFolder + '/containsWord.csv', 'w')
    sentimentFile = open(outputFolder + '/sentiment.csv', 'w')
    tweetsFile = open(outputFolder + '/tweets.json', 'w')

    # build stop word list
    # englishStopWords = [normalize_str(w).lower() for w in stopwords.words('english')]
    # spanishStopWords = [normalize_str(w).lower() for w in stopwords.words('spanish')]
    # stopWordList = []
    # stopWordList.extend(englishStopWords)
    # stopWordList.extend(spanishStopWords)

    log.info("# of keywords: " + str(len(keywordList)))
    log.info("tracking--> " + str(keywordList))
    # build regular expression for keyword
    #keywordRegex = re.compile(r'\b%s\b' % '\\b|\\b'.join(keywordList),
    #                          flags=re.IGNORECASE)
    # for geocoding tweets
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

                                #keywordsPresent = re.findall(keywordRegex, text)
                                #keywordsPresent = list(set(keywordsPresent))
                                #if len(keywordsPresent) > 0:

                                tweetId = jsonTweet["twitter"]["id"]
                                tweeterId = str(jsonTweet["interaction"]["author"]["id"])
                                mentions = getInteractions(jsonTweet)
                                sentiment = getSentiment(jsonTweet)
                                if sentiment == 0:
                                    continue

                                hashTags = extract_hash_tags(text)
                                hashTags = [hashTag for hashTag in hashTags if len(hashTag) > 3]
                                #hashTags.extend(keywordsPresent)
                                if len(hashTags) == 0:
                                    continue
                                hashTags = list(set(hashTags))

                                tweetedFile.write(tweeterId + ',' + tweetId + '\n')
                                sentimentFile.write(tweetId + ',' + str(sentiment) + '\n')
                                for userId in mentions:
                                    mentionFile.write(tweetId + ',' + userId + '\n')
                                # for userId in retweets:
                                #     retweetFile.write(tweetId + ',' + userId + '\n')
                                for hashTag in hashTags:
                                    if hashTag.startswith('#'):
                                        hashTag = hashTag[1:]
                                    wordsFile.write(tweetId + ',' + hashTag + '\n')
                                # tracking the tweets for checks.
                                if tweeterId in tweets:
                                    tweets[tweeterId][tweetId] = jsonTweet["interaction"]["content"]
                                else:
                                    tweets[tweeterId] = {}
                                    tweets[tweeterId][tweetId] = jsonTweet["interaction"]["content"]

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

    tweetsFile.write(json.dumps(tweets, indent=4))
    tweetsFile.close()
    mentionFile.close()
    # retweetFile.close()
    wordsFile.close()
    tweetedFile.close()
    sentimentFile.close()
    return


def executePSLCode(inputFolder, outputFolder, classPathFile):
    commandToExecute = "java -Xmx12g -cp /home/aravindan/Dropbox/git/ms_thesis/psl/electionsLDA/target/classes:`cat " + classPathFile + "` edu.umd.cs.linqs.embers.electionsLDA " + inputFolder + " " + outputFolder
    # log.info("\n\n" + commandToExecute + "\n\n")
    os.system(commandToExecute)
    return


def makePredictions(dataFolder, fromDate, toDate, userThreshold, configFile, regressionFlag):
    log.info('making predictions')

    folders = sorted(os.listdir(dataFolder))
    affiliation = {}
    votes = {}

    for dateStr in folders:
        date = datetime.strptime(dateStr + fromDate.strftime('%Y'), "%d%b%Y")
        if date >= fromDate and date <= toDate:
            # get the new user affiliations first
            print ("processing date--->" + date.strftime("%d %b %y"))
            statsFolder = dataFolder + '/' + dateStr + '/stats'
            for file in os.listdir(statsFolder):
                group, part = file.split('_')
                if group not in votes:
                    votes[group] = 0
                if part.startswith('supporters'):
                    with open(statsFolder + '/' + file, 'r') as _file:
                        for line in _file:
                            user, weight = line.strip().split(',')
                            if user not in affiliation:
                                affiliation[user] = {}
                                affiliation[user][group] = weight
                            else:
                                affiliation[user][group] = weight
            #if regressionFlag == '1':

    #debugging
    oneZero, twoZero, bothZero, oneAbove, twoAbove, bothAbove = 0, 0, 0, 0, 0, 0
    for user in affiliation:
        try:
            if affiliation[user]['chavez'] == 0:
                    oneZero += 1
            if affiliation[user]['capriles'] == 0:
                twoZero += 1
            if affiliation[user]['capriles'] == 0 and affiliation[user]['chavez'] == 0:
                bothZero += 1
            if affiliation[user]['chavez'] >= userThreshold:
                oneAbove += 1
            if affiliation[user]['capriles'] >= userThreshold:
                twoAbove += 1
            if affiliation[user]['capriles'] >= userThreshold and affiliation[user]['chavez'] >= userThreshold:
                bothAbove += 1
        except:
            if 'chavez' not in affiliation[user]:
                oneZero += 1
            if 'capriles' not in affiliation[user]:
                twoZero += 1
    print "debugging statement"
    print("( %d, %d, %d, %d, %d, %d)" % (oneZero, twoZero, bothZero, oneAbove, twoAbove, bothAbove))
    #debugging
    # count votes
    for user in affiliation:
        max = 0
        for group in affiliation[user]:
            if affiliation[user][group] > userThreshold and affiliation[user][group] > max:
                    votes[group] += 1

    totalVotes = len(affiliation.keys())

    print ("*****************************PSL RESULTS*******************")
    print ('total votes ----> ' + str(totalVotes))
    for group in votes:
        print group + ' got ----> ' + str((votes[group] / totalVotes) * 100) + ' %'
    return


def execute(arg):
    logs.init(arg)
    log.info("*************************************")
    log.info("PSL 4 Elections pipeline initializing")
    log.info("tweet folder------> " + arg.tweetFolder)
    log.info("dataFolder--------> " + str(arg.dataFolder))
    log.info("fromDate----------> " + arg.fromDate)
    log.info("toDate------------> " + arg.toDate)
    log.info("window------------> " + str(arg.window) + " day(s)")
    log.info("userThreshold-----> " + str(arg.userThreshold))
    log.info("wordThreshold-----> " + str(arg.wordThreshold))
    log.info("makePredictions---> " + arg.predictionFlag)
    log.info("*************************************")

    fromDate = datetime.strptime(arg.fromDate, "%d %b %Y")
    toDate = datetime.strptime(arg.toDate, "%d %b %Y")
    currentDate = fromDate

    iterCount = 1
    membership = {}
    likes = {}
    hates = {}

    if arg.predictionFlag == '1':
        makePredictions(arg.dataFolder + '/' + arg.country, fromDate, toDate, arg.userThreshold, arg.configFile, arg.regressionFlag)
        sys.exit()

    while(currentDate <= toDate):
        log.info("iterCount--------------->" + str(iterCount))
        log.info("processing PSL pipeline for %s" % (currentDate.strftime("%d %b %Y")))

        log.debug("creating the directory substructure for current date")
        inputFolder = arg.dataFolder + '/' + arg.country + '/' + currentDate.strftime("%d%b") + '/inputs'
        outputFolder = arg.dataFolder + '/' + arg.country + '/' + currentDate.strftime("%d%b") + '/outputs'
        statsFolder = arg.dataFolder + '/' + arg.country + '/' + currentDate.strftime("%d%b") + '/stats'
        os.system('mkdir -p ' + inputFolder)
        os.system('mkdir -p ' + outputFolder)
        os.system('mkdir -p ' + statsFolder)

        nextDate = currentDate + timedelta(days=arg.window)
        if(nextDate <= toDate):
            log.debug("creating the directory substructure for next date")
            nextInputFolder = arg.dataFolder + '/' + arg.country + '/' + nextDate.strftime("%d%b") + '/inputs'
            nextOutputFolder = arg.dataFolder + '/' + arg.country + '/' + nextDate.strftime("%d%b") + '/outputs'
            nextStatsFolder = arg.dataFolder + '/' + arg.country + '/' + nextDate.strftime("%d%b") + '/stats'
            os.system('mkdir -p ' + nextInputFolder)
            os.system('mkdir -p ' + nextOutputFolder)
            os.system('mkdir -p ' + nextStatsFolder)

        if iterCount == 1:
            keywordList = []
        ## adding seed words to the list
        #seedWordList = []
        #with open(arg.seedFile, 'r') as file:
        #    for line in file:
        #        word, group, weight = line.split(',')
        #        seedWordList.append(word)
        #else:
        #    keywordList = list(set(keywordList).union(set(seedWordList)))

        log.info("copying the seedFile to inputFolder")
        os.system('cp ' + arg.seedFile + ' ' + inputFolder + '/seedWords.csv')

        preProcess(arg.tweetFolder, inputFolder, keywordList, currentDate, nextDate, arg.country)
        log.info("***********preProcess complete******************")

        executePSLCode(inputFolder, outputFolder, arg.classPathFile)
        log.info("***********PSL code complete********************")

        postProcess(outputFolder, nextInputFolder, statsFolder, arg.userThreshold, arg.wordThreshold, membership, likes, hates)
        log.info("**********postProcess complete*****************")

        log.info("deleting the database used for current iteration")
        os.system('rm /home/aravindan/Dropbox/git/ms_thesis/psl/electionLDADB*')

        iterCount += 1
        currentDate = nextDate
        log.info("**********************************************************")

if __name__ == "__main__":
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
    ap.add_argument('-cf', '--configFile', type=str,
                    help='electionConfigFile for prediction',
                    default='/home/aravindan/Dropbox/git/ms_thesis/configFiles/electionConfig_VE')
    ap.add_argument('-cp', '--classPathFile', type=str,
                    help='file containing class path for PSL execution',
                    default='/home/aravindan/Dropbox/git/ms_thesis/psl/classPathFile')
    ap.add_argument('-r', '--regressionFlag', type=str,
                    default='0')
    ap.add_argument('-pf', '--predictionFlag', type=str,
                    help='flag to predict or run pipeline',
                    default='1')
    arg = ap.parse_args()

    execute(arg)
