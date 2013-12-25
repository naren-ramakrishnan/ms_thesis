#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
__author__ = "Aravindan Mahendiran"
__email__ = "aravind@vt.edu"
__processor__ = "PSL4ElectionsPipeline"
__version__ = "6.0.0"

'''
Script to pipeline the entire PSL process for
election modeling.
tracks hashTags only
each window is a separate iteration
preprocess uses tweets that match the keyword/ member filter
mentions and retweets are encoded as two separate predicates
likes and hates are encoded as belongsTo and ~belongsTo
'''

import sys
import os
import traceback
from etool import logs, args
import json
import re
from datetime import datetime, timedelta
path = "/home/aravindan/Dropbox/git/geocoding/twitter"
if path not in sys.path:
    sys.path.insert(1, path)
from embers.geocode import Geo
from embers.utils import normalize_str
from sklearn import linear_model
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
    mentions = list(set(mentions))
    retweets = list(set(retweets))
    return mentions, retweets


def postProcess(inputFolder, outputFolder, userThreshold, wordThreshold, membership, vocab):
    log.debug("inside postProcess")

    memberFile = inputFolder + '/isMember.tsv'
    belongsToFile = inputFolder + '/belongsTo.tsv'
    groups = []
    keywordList = []
    memberList = []
    wasMemberFile = outputFolder + '/wasMember.csv'
    belongedToFile = outputFolder + '/belongedTo.csv'

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
                if group not in groups:
                    groups.append(group)
                if word in vocab:
                    vocab[word][group] = weight
                else:
                    vocab[word] = {}
                    vocab[word][group] = weight
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
                if group not in groups:
                    groups.append(group)
                if weight > 1.0:
                    weight = 1.0
                if user in membership:
                    membership[user][group] = weight
                else:
                    membership[user] = {}
                    membership[user][group] = weight
            except Exception, e:
                log.exception('error while processing memberFile %s', str(e))

    log.debug('groups[] -->' + str(groups))
    log.debug('len(membership)-->' + str(len(membership)))
    log.debug('len(vocab)-->' + str(len(vocab)))
    log.debug('vocab-->' + str(vocab))

    # writing the wasMember file for next iteration of PSL
    with open(wasMemberFile, 'w') as file:
        for user in membership:
            maxWeight = 0
            supports = None
            for group in groups:
                if group in membership[user] and membership[user][group] > maxWeight and membership[user][group] >= userThreshold:
                    maxWeight = membership[user][group]
                    supports = group
            if supports is not None:
                file.write(user + ',' + supports + ',' + str(maxWeight) + '\n')
                memberList.append(user)

    # writing the belongedToFile for next iteration of PSL
    with open(belongedToFile, 'w') as file:
        for word in vocab:
            maxWeight = 0
            belongs = None
            for group in groups:
                if group in vocab[word] and vocab[word][group] > maxWeight and vocab[word][group] >= wordThreshold:
                    maxWeight = vocab[word][group]
                    belongs = group
            if belongs is not None:
                file.write(word + ',' + belongs + ',' + str(maxWeight) + '\n')
                keywordList.append(word)

    return keywordList, memberList


def preProcess(tweetFolder, outputFolder, keywordList, memberList, fromDate, toDate, country):
    log.debug('inside preProcess')
    log.debug("fromDate-->" + fromDate.strftime("%d %b %Y"))
    log.debug("toDate-->" + toDate.strftime("%d %b %Y"))

    tweets = {}

    # output files
    tweetedFile = open(outputFolder + '/tweeted.csv', 'w')
    mentionFile = open(outputFolder + '/mentioned.csv', 'w')
    retweetFile = open(outputFolder + '/retweet.csv', 'w')
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
    log.debug("tracking--> " + str(keywordList))
    # build regular expression for keyword
    keywordRegex = re.compile(r'\b%s\b' % '\\b|\\b'.join(keywordList),
                              flags=re.IGNORECASE)
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
                            memberFlag = False
                            jsonTweet = json.loads(line.strip())
                            dateStr = jsonTweet['interaction']['created_at'][5:16]
                            tweetDate = datetime.strptime(dateStr, '%d %b %Y')
                            sentiment = getSentiment(jsonTweet)
                            geoList = geo.geo_normalize(jsonTweet)
                            city, ctry, state = geoList[:3]
                            if ctry and (ctry.lower() == country) and (tweetDate >= fromDate) and (tweetDate <= toDate):
                                # prereProcess the tweet
                                text = jsonTweet["interaction"]["content"]
                                text = re.sub(URL_REGEX, ' ', text)  # remove urls
                                text = re.sub('[^A-Za-z_@#0-9]', ' ', normalize_str(text, lower=True))  # allow only alphaNumerics and twitter tags
                                text = re.sub(' +', ' ', text)  # remove multiple spaces
                                tweeterId = str(jsonTweet["interaction"]["author"]["id"])
                                mentions, retweets = getInteractions(jsonTweet)
                                uids = []
                                uids.extend(mentions)
                                uids.extend(retweets)
                                uids.append(tweeterId)
                                uids = list(set(uids))
                                for uid in uids:
                                    if uid in memberList:
                                        memberFlag = True
                                        break
                                keywordsPresent = re.findall(keywordRegex, text)
                                keywordsPresent = list(set(keywordsPresent))
                                if len(keywordsPresent) > 0 or memberFlag:
                                    log.debug('this tweet passed the filter')
                                    hashTags = extract_hash_tags(text)
                                    tweetId = jsonTweet["twitter"]["id"]
                                    hashTags = [hashTag for hashTag in hashTags if len(hashTag) > 3]
                                    hashTags.extend(keywordsPresent)
                                    if len(hashTags) == 0:
                                        continue
                                    log.debug('tweet used for PSL')
                                    hashTags = list(set(hashTags))
                                    tweetedFile.write(tweeterId + ',' + tweetId + '\n')
                                    sentimentFile.write(tweetId + ',' + str(sentiment) + '\n')
                                    for userId in mentions:
                                        mentionFile.write(tweetId + ',' + userId + '\n')
                                    for userId in retweets:
                                        retweetFile.write(tweetId + ',' + userId + '\n')
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
                            _traceback = sys.exc_info()[2]
                            log.debug("error processing tweet %s @line %s" % (f, traceback.tb_lineno(_traceback)))
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
    retweetFile.close()
    wordsFile.close()
    tweetedFile.close()
    sentimentFile.close()
    return


def executePSLCode(inputFolder, outputFolder, classPathFile):
    commandToExecute = "java -Xmx12g -cp /home/aravindan/Dropbox/git/ms_thesis/psl/ElectionsPSL/target/classes:`cat " + classPathFile + "` edu.umd.cs.linqs.embers.electionsPSL " + inputFolder + " " + outputFolder
    log.debug("\n\n" + commandToExecute + "\n\n")
    os.system(commandToExecute)
    return


def countVotes(dataFolder, fromDate, toDate, userThreshold):
    log.debug('counting Votes')

    folders = os.listdir(dataFolder)
    folders = sorted(folders, key=lambda x: datetime.strptime(x, '%d%b'))
    affiliation = {}
    votes = {}

    for dateStr in folders:
        date = datetime.strptime(dateStr + fromDate.strftime('%Y'), "%d%b%Y")
        if date >= fromDate and date <= toDate:
            # get the new user affiliations first
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
    # count votes
    totalVotes = len(affiliation.keys())
    for user in affiliation:
        max = 0
        voteFor = None
        for group in affiliation[user]:
            if affiliation[user][group] > max and affiliation[user][group] > userThreshold:
                voteFor = group
        votes[voteFor] += 1
    for group in votes:
        votes[group] = votes[group] / totalVotes * 100
    return votes


def makePredictions(dataFolder, fromDate, toDate, userThreshold, configFile):
    configJson = json.loads(open(configFile, 'r').read())['candidates']
    #regressionFit
    for candidate in configJson:
        dateList = []
        for dateStr in configJson[candidate]['polls'].keys():
            date = datetime.strptime(dateStr, "%d %b %Y")
            if date >= fromDate and date <= toDate:
                dateList.append(dateStr)
        dateList = sorted(dateList, key=lambda x: datetime.strptime(x, '%d %b %Y'))
        Y_train = []
        X_train = []
        for dateStr in dateList:
            Y_train.append(configJson[candidate]['polls'][dateStr])
            localToDate = datetime.strptime(dateStr, '%d %b %Y')
            x0, x1 = [1] * 2
            votes = countVotes(dataFolder, fromDate, localToDate, userThreshold)
            x1 = votes[candidate]
            X_train.append([x0, x1])
        regression = linear_model.Lasso(alpha=0.1, normalize=True)
        regression.fit(X_train, Y_train)
        X_predict = []
        x0, x1 = [1] * 2
        votes = countVotes(dataFolder, fromDate, toDate, userThreshold)
        x1 = votes[candidate]
        X_predict.append([x0, x1])
        votes[candidate] = regression.predict(X_predict)[0]

    return votes


def execute(arg):
    logs.init(arg)
    log.info("*************************************")
    log.info("PSL 4 Elections pipeline initializing")
    log.info("tweet folder------> " + arg.tweetFolder)
    log.info("dataFolder--------> " + str(arg.dataFolder))
    log.info("fromDate----------> " + arg.fromDate)
    log.info("toDate------------> " + arg.toDate)
    log.info("window------------> " + str(arg.window) + " day(s)")
    log.info("country------------> " + arg.country)
    log.info("userThreshold-----> " + str(arg.userThreshold))
    log.info("wordThreshold-----> " + str(arg.wordThreshold))
    log.info("makePredictions---> " + arg.predictionFlag)
    log.info("*************************************")

    fromDate = datetime.strptime(arg.fromDate, "%d %b %Y")
    toDate = datetime.strptime(arg.toDate, "%d %b %Y")
    currentDate = fromDate

    iterCount = 1
    membership = {}
    vocab = {}

    if arg.predictionFlag == '1':
        dataFolder = arg.dataFolder + '/' + arg.country
        votes = makePredictions(dataFolder, fromDate, toDate, arg.userThreshold, arg.configFile)
        for candidate in votes:
            print (candidate + ' got --> ' + str(votes[candidate]) + '%')
        log.info("predictionsMade.....exiting")
        sys.exit()

    while(currentDate <= toDate):
        log.info("iterCount--------------->" + str(iterCount))
        log.info("processing PSL pipeline for %s" % (currentDate.strftime("%d %b %Y")))

        log.debug("creating the directory substructure for current date")
        inputFolder = arg.dataFolder + '/' + arg.country + '/' + currentDate.strftime("%d%b") + '/inputs'
        outputFolder = arg.dataFolder + '/' + arg.country + '/' + currentDate.strftime("%d%b") + '/outputs'
        os.system('mkdir -p ' + inputFolder)
        os.system('mkdir -p ' + outputFolder)

        nextDate = currentDate + timedelta(days=arg.window)
        if(nextDate <= toDate):
            log.debug("creating the directory substructure for next date")
            nextInputFolder = arg.dataFolder + '/' + arg.country + '/' + nextDate.strftime("%d%b") + '/inputs'
            nextOutputFolder = arg.dataFolder + '/' + arg.country + '/' + nextDate.strftime("%d%b") + '/outputs'
            os.system('mkdir -p ' + nextInputFolder)
            os.system('mkdir -p ' + nextOutputFolder)

        if iterCount == 1:
            keywordList = []
            memberList = []
            # adding seed words to the list
            seedWordList = []
            with open(arg.seedFile, 'r') as file:
                for line in file:
                    word, group, weight = line.split(',')
                    seedWordList.append(word)
        keywordList = list(set(keywordList).union(set(seedWordList)))

        log.info("copying the seedFile to inputFolder")
        os.system('cp ' + arg.seedFile + ' ' + inputFolder + '/belongedTo.csv')

        try:
            preProcess(arg.tweetFolder, inputFolder, keywordList, memberList, currentDate, nextDate, arg.country)
            log.info("***********preProcess complete******************")
        except Exception, e:
            _traceback = sys.exc_info()[2]
            log.exception('error during preProcess %s @line %s' % (e, traceback.tb_lineno(_traceback)))

        try:
            executePSLCode(inputFolder, outputFolder, arg.classPathFile)
            log.info("***********PSL code complete********************")
        except Exception, e:
            _traceback = sys.exc_info()[2]
            log.exception('error during execution %s @line %s' % (e, traceback.tb_lineno(_traceback)))

        try:
            keywordList, memberList = postProcess(outputFolder, nextInputFolder, arg.userThreshold, arg.wordThreshold, membership, vocab)
            log.info("**********postProcess complete*****************")
        except Exception, e:
            _traceback = sys.exc_info()[2]
            log.exception('error during postProcess %s @line %s' % (e, traceback.tb_lineno(_traceback)))

        log.info("deleting the database used for current iteration")
        os.system('rm /home/aravindan/Dropbox/git/ms_thesis/psl/electionPSLDB*')

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
