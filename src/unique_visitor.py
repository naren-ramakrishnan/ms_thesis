#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from __future__ import division

__author__ = 'Aravindan Mahendiran'
__email__ = 'aravind@vt.edu'
__version__ = "5.2.0"
__processor__ = "UniqueVisitorElectionModel"

import sys
import os
import re
import json
import boto
from datetime import datetime
from embers.utils import normalize_str
from etool import logs, args, message

path = "/home/aravindan/Dropbox/git/geocoding/twitter"
if path not in sys.path:
    sys.path.insert(1, path)
from embers.geocode import Geo

log = logs.getLogger(__processor__)


def scaleSentiment(value):
    """changing scale from (-15, 15) to (1, 31)"""
    return value + 16


def getKloutSentiment(jsonTweet):
    try:
        klout = jsonTweet["klout"]["score"]
    except Exception, e:
        log.debug("error getting Klout:%s, setting klout=1", e)
        klout = 1
    try:
        sentiment = jsonTweet["salience"]["content"]["sentiment"]
    except Exception, e:
        sentiment = 0
        log.debug("error getting sentiment:%s, setting sentiment=0", str(e))
    return klout, sentiment


class Elections(object):
    """
    Class to count candidate mentions from twitter and predict election winner
    """
    def __init__(self, inputFolder, scoresFolder, configFile, fromDate, toDate):
        log.info("initializing")
        self.configJson = None
        with open(configFile, "r") as f:
            self.configJson = json.loads(f.read())

        self.country = self.configJson["country"]
        self.state = self.configJson["state"]
        self.city = self.configJson["city"]
        self.electionDate = self.configJson["date"]
        self.electionType = self.configJson["type"]
        self.runOff = self.configJson["runOff"]

        self.fromDate = datetime.strptime(fromDate, "%d %b %Y")
        self.toDate = datetime.strptime(toDate, "%d %b %Y")
        self.inputFolder = inputFolder

        log.info("Election For: (" + self.country + ',' + self.state + ',' + self.city + ')')
        self.scoresFolder = scoresFolder
        self.candidatesJson, self.aliasJson = self.getInfo()
        self.rePattern = re.compile(r'\b%s\b' % '\\b|\\b'.join(
                                    self.aliasJson.keys()),
                                    flags=re.IGNORECASE)
        self.initScoreStructure()
        log.debug("scoreCard structure-->%s" % self.scoreCard)

        return

    def getInfo(self):
        log.debug("processing the config file")
        candidatesJson, aliasJson = {}, {}
        for candidate in self.configJson["candidates"]:
            candidatesJson[candidate] = self.configJson["candidates"][candidate]
        for candidate in candidatesJson:
            for alias in candidatesJson[candidate]["alias"]:
                if alias.lower() in aliasJson:
                    aliasJson[alias.lower()].append(candidate)
                else:
                    aliasJson[alias.lower()] = [candidate]
        return candidatesJson, aliasJson

    def initScoreStructure(self):
        """ intializes the scoreCard and the tweetList"""
        try:
            with open(self.scoresFolder + "/scoreCard", "r") as f:
                self.scoreCard = json.loads(f.read())
            log.info("scoreCard loaded")
        except Exception:
            # initialize for first time
            log.info("Initializing new scoreCard")
            self.scoreCard = {}
            for candidate in self.candidatesJson:
                self.scoreCard[candidate] = {}
        try:
            with open(self.scoresFolder + "/tweetList", "r") as g:
                self.tweetList = json.loads(g.read())
            log.info("tweetList loaded")
        except Exception:
            # initialize for first time
            log.info("initializing new tweetList")
            self.tweetList = []
        return

    def updateScoreCard(self, candidatesFound, tweeterId, klout, sentiment, date):
        scoreTuple = (klout, sentiment)
        for candidate in candidatesFound:
            if tweeterId in self.scoreCard[candidate]:
                if date in self.scoreCard[candidate][tweeterId]:
                    self.scoreCard[candidate][tweeterId][date].append(scoreTuple)
                else:
                    self.scoreCard[candidate][tweeterId][date] = []
                    self.scoreCard[candidate][tweeterId][date].append(scoreTuple)
            else:
                self.scoreCard[candidate][tweeterId] = {}
                self.scoreCard[candidate][tweeterId][date] = []
                self.scoreCard[candidate][tweeterId][date].append(scoreTuple)
        return

    def storeScoreCard(self):
        log.info("storing scorecard")
        with open(self.scoresFolder + "/scoreCard", "w") as f:
            f.write(json.dumps(self.scoreCard))
        log.info("storing tweetList")
        with open(self.scoresFolder + "/tweetList", "w") as f:
            f.write(json.dumps(self.tweetList))
        return

    def normalizeScores(self, scoreList):
        summation = 0
        for candidate in scoreList:
            summation += scoreList[candidate]
        for candidate in scoreList:
            scoreList[candidate] = scoreList[candidate] / summation * 100
        return scoreList

    def getBestTweeterTuple(self, tweeterJson):
        bestSentiment = 1
        for date in tweeterJson:
            for tuple in tweeterJson[date]:
                klout, sentiment = tuple
                if sentiment > bestSentiment:
                    bestSentiment = sentiment
        return (klout, bestSentiment)

    def getAvgTweeterTuple(self, tweeterJson, fromDate, toDate):
        count = 0
        sumSentiment = 0
        fromDate = datetime.strptime(fromDate, "%d %b %Y")
        toDate = datetime.strptime(toDate, "%d %b %Y")
        klout, avgSentiment = 0, 0
        for dateStr in tweeterJson:
            date = datetime.strptime(dateStr, "%d %b %Y")
            if (date >= fromDate and date <= toDate):
                for klout, sentiment in tweeterJson[dateStr]:
                    sumSentiment += sentiment
                    count += 1
        if count == 0:
            avgSentiment = sumSentiment / 1
        else:
            avgSentiment = sumSentiment / count
        return(klout, avgSentiment)

    def getFinalScores(self, fromDate, toDate):
        finalScore = {}
        for candidate in self.candidatesJson:
            sumScore = 1
            for tweeterId in self.scoreCard[candidate]:
                klout, sentiment = self.getAvgTweeterTuple(self.scoreCard[candidate][tweeterId], fromDate, toDate)
                sumScore += klout * scaleSentiment(sentiment)
            finalScore[candidate] = self.candidatesJson[candidate]["weight"] * sumScore
            #finalScore[candidate] = sumScore
            if self.candidatesJson[candidate]['incumbent'] == 'Y':
                finalScore[candidate] = 1.10 * finalScore[candidate]
        return finalScore

    def getWinner(self, fromDate, toDate):
        finalScores = self.getFinalScores(fromDate, toDate)
        finalScores = self.normalizeScores(finalScores)
        winningScore, runnerUpScore = 0, 0
        winner, runnerUp = None, None
        for candidate in finalScores:
            if finalScores[candidate] > winningScore:
                winningScore = finalScores[candidate]
                winner = candidate
        #for runOff elections
        for candidate in finalScores:
            if candidate != winner and finalScores[candidate] >= runnerUpScore:
                runnerUpScore = finalScores[candidate]
                runnerUp = candidate
        log.info("finalScore-->\n%s" % finalScores)
        return winner, winningScore, runnerUp, runnerUpScore, finalScores

    def processTweet(self, jsonTweet):
        try:
            text = jsonTweet["interaction"]["content"]
            text = re.sub("[^A-Za-z_@#0-9]", " ", normalize_str(text))
            searchResult = self.rePattern.findall(text)
            candidateList = []
            if searchResult:
                self.tweetList.append(jsonTweet["embersId"])
                for term in searchResult:
                    for candidate in self.aliasJson[term]:
                        candidateList.append(candidate)
                log.debug("matched aliases---> %s" % candidateList)
            return set(candidateList)
        except Exception, e:
            raise e

    def collectMentions(self):
        geo = Geo()
        tweetCount = 0
        tweetErrorCount = 0
        totalFiles = len(os.listdir(self.inputFolder))
        fileCount = 1
        for _file in sorted(os.listdir(self.inputFolder)):
            fileDate = datetime.strptime(_file[17:27], "%Y-%m-%d")
            if(fileDate > self.toDate or fileDate < self.fromDate):
                continue
            log.info("processing file %d/%d-->%s" % (fileCount, totalFiles, _file))
            fileCount += 1
            try:
                with open(self.inputFolder + "/" + _file, "r") as FILE:
                    for line in FILE:
                        try:
                            jsonTweet = json.loads(line.strip())
                            geoList = geo.geo_normalize(jsonTweet)
                            city = geoList[0]
                            country = geoList[1]
                            state = geoList[2]
                            if ((self.city == '-' and self.state == '-' and country and country.lower() == self.country) or
                                    (country and country.lower() == self.country and state and state.lower() == self.state) or
                                    (country and country.lower() == self.country and state and state.lower() == self.state and city and city.lower() == self.city)):
                                tweetCount += 1
                                # use [5:25] if need HH:MM:SS
                                datestr = jsonTweet["interaction"]["created_at"][5:16]
                                klout, sentiment = getKloutSentiment(jsonTweet)
                                tweeterId = jsonTweet["interaction"]["author"]["id"]
                                candidatesFound = self.processTweet(jsonTweet)
                                self.updateScoreCard(candidatesFound,
                                                     tweeterId, klout,
                                                     sentiment, datestr)
                        except Exception, f:
                            log.exception("error processing tweets %s", f)
                            tweetErrorCount += 1
            except Exception, e:
                log.exception("error processfing file %s", e)
        log.debug("tweets used: %s" % str(tweetCount))
        log.debug("tweetErrorCount : %s" % str(tweetErrorCount))
        self.storeScoreCard()
        return

    def createSurrogate(self, winner, winningScore, runnerUp, runnerUpScore, pushFlag):
        surrogate = {}
        surrogate["date"] = datetime.utcnow().isoformat('T')
        surrogate["scores"] = self.scoreCard
        surrogate["model"] = "VoteSentimentEvolutionModel"
        surrogate["derivedFrom"] = {}
        surrogate["derivedFrom"]["derivedIds"] = self.tweetList
        surrogate["derivedFrom"]["location"] = [self.country.title(),
                                                self.state.title(),
                                                self.city.title()]
        surrogate["derivedFrom"]["source"] = "Raw Twitter feed from DataSift and election config Files."
        surrogate["derivedFrom"]["comments"] = "tweets were filtered by (country,state,city) and then by those containing the terms of candidates"
        surrogate["derivedFrom"]["model"] = __processor__
        surrogate["confidence"] = 1.00
        surrogate["confidenceIsProbability"] = True
        surrogate["configuration"] = self.configJson
        surrogate = message.add_embers_ids(surrogate)
        log.info("surrogate--->\n%s" % surrogate)

        #writing surrogate to file
        if self.state == '-':
            surrogateFileName = "surrogate_" + self.country + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        elif self.city == '-':
            surrogateFileName = "surrogate_" + self.state + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        else:
            surrogateFileName = "surrogate_" + self.city + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        with open("../data/surrogates/" + surrogateFileName, "w") as f:
            f.write(json.dumps(surrogate))
        log.info("surrogate written to file")

        # writing files to s3
        if pushFlag == '1':
            try:
                with open("../data/surrogates/" + surrogateFileName, 'r') as f:
                    pushFileToS3(f, 'surrogates/elections/' + surrogateFileName)
                log.info("surrogate pushed to s3")
            except:
                log.exception("surrogate push to S3 failed!! push manually!!")
        self.createWarning(surrogate["embersId"], winner, winningScore, runnerUp, runnerUpScore, pushFlag)
        return

    def createWarning(self, surrogateId, winner, winningScore, runnerUp, runnerUpScore, pushFlag):
        warning = {}
        comment = "Election model v6"
        warning["date"] = datetime.utcnow().isoformat('T')
        warning["derivedFrom"] = {}
        warning["derivedFrom"]["derivedIds"] = [surrogateId]
        warning["derivedFrom"]["location"] = [self.country.title(),
                                              self.state.title(),
                                              self.city.title()]
        warning["derivedFrom"]["source"] = "Raw Twitter feed from DataSift and election config Files."
        warning["derivedFrom"]["model"] = __processor__
        warning["model"] = "VoteSentimentEvolutionModel v6"
        electionType = ''
        if self.electionType in ['president', 'prime minister']:
            electionType = 'President/Prime Minister'
        else:
            electionType = self.electionType
        warning["eventType"] = ["Vote", "Election", electionType.title()]
        warning["confidence"] = round(winningScore / 100, 2)  # in cases of very close run races
        #warning["confidence"] = 1.00
        warning["confidenceIsProbability"] = True
        warning["eventDate"] = self.electionDate
        warning["population"] = winner.title()
        warning["location"] = [self.country.title(), self.state.title(),
                               self.city.title()]
        comment = "Winner: " + winner + " Score: " + str(winningScore) + " Runner-Up: " + runnerUp + " Score: " + str(runnerUpScore)
        if self.runOff == 'Y':
            comment = "Election going to 2nd Round with: " + winner + " and " + runnerUp
        warning["comments"] = comment
        warning['eventCode'] = '02'
        if self.electionType == 'mayor':
            warning['eventCode'] = '0213'
        elif self.electionType in ['president', 'prime minister']:
            warning['eventCode'] = '0211'
        elif self.electionType == 'governor':
            warning['eventCode'] = '0212'
        warning = message.add_embers_ids(warning)
        log.info("warning-->\n%s" % warning)

        # writing warning to file
        if self.state == '-':
            warningFileName = "warning_" + self.country + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        elif self.city == '-':
            warningFileName = "warning_" + self.state + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        else:
            warningFileName = "warning_" + self.city + "_" + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        with open("../data/warnings/" + warningFileName, "w") as f:
            f.write(json.dumps(warning))
        log.info("warning written to file")

        # writing files to s3
        if pushFlag == '1':
            try:
                with open("../data/warnings/" + warningFileName, 'r') as f:
                    pushFileToS3(f, 'incoming/predictions/elections/' +
                                 warningFileName)
                log.info("warning pushed to s3")
            except:
                log.exception("warning push to S3 failed!! push manually!!")
        return


def pushFileToS3(fileHandle, path):
    connection = boto.connect_s3(os.environ['AWS_ACCESS_KEY_ID'],
                                 os.environ['AWS_SECRET_ACCESS_KEY'])
    bucket = connection.get_bucket('embers-osi-data')
    k = boto.s3.key.Key(bucket)
    k.key = path
    k.set_contents_from_file(fileHandle)
    return


def main():
    ap = args.get_parser()
    ap.add_argument('-i', '--inputFolder', type=str,
                    help='inputFolder contaning twitter files',
                    default='/hdd/tweets/2012/may')
    ap.add_argument('-s', '--scoresFolder', type=str,
                    help='Folder contaning scoreCards',
                    default='../data/scores/')
    ap.add_argument('-cf', '--configFile', type=str,
                    help='election configuration file',
                    default='./configFiles/electionConfig_MX')
    ap.add_argument('-d1', '--fromDate', help='fromDate')
    ap.add_argument('-d2', '--toDate', help='toDate')
    ap.add_argument('-f1', '--flag1', help="countOrPredict",
                    type=str, default='2')
    ap.add_argument('-f2', '--flag2', help="flag to push the warnings to S3",
                    type=str, default='0')
    arg = ap.parse_args()
    logs.init(arg)

    try:
        elections = Elections(arg.inputFolder, arg.scoresFolder,
                              arg.configFile, arg.fromDate, arg.toDate)
        log.info("Election class initialized")
    except Exception as e:
        log.exception("exception during intialization: %s. Quitting!!", e)

    try:
        if (arg.flag1 == '1' or arg.flag1 == '3'):
            elections.collectMentions()
    except Exception as e:
        log.exception("error while tracking tweets")

    try:
        if (arg.flag1 == '2' or arg.flag1 == '3'):
            winner, winningScore, runnerUp, runnerUpScore, finalScore = elections.getWinner(arg.fromDate, arg.toDate)
            print "---------Results using Unique Visitor Model----------------------------------------------"
            print finalScore
            print winner + "--->" + str(winningScore)
            print "-------------------------------------------------------------------------------"
    except Exception as e:
        log.exception("error while calculating winner:%s", e)

    try:
        elections.createSurrogate(winner, winningScore, runnerUp, runnerUpScore, arg.flag2)
    except Exception as e:
        log.exception("error during creating warnings")

    log.info("ALL Operations Complete")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print e
