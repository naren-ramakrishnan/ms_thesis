#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from __future__ import division

__author__ = 'Aravindan Mahendiran'
__email__ = 'aravind@vt.edu'
__version__ = "6.3.1"
__processor__ = "RegressionElectionModel"

import sys
import os
import re
import json
import boto
from datetime import datetime, timedelta
from embers.utils import normalize_str
from etool import logs, args, message
path = "/home/aravindan/Dropbox/git/geocoding/twitter"
if path not in sys.path:
    sys.path.insert(1, path)
from embers.geocode import Geo
from sklearn import linear_model

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
            if "polls" in self.configJson["candidates"][candidate]:
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

    def getStatistics(self, fromDate, toDate):
        log.info("calculating statistics")
        fromDate = datetime.strptime(fromDate, "%d %b %Y")
        toDate = datetime.strptime(toDate, "%d %b %Y")
        self.statistics = {}
        for candidate in self.candidatesJson:
            self.statistics[candidate] = {}
            vol_total, vol_pos, vol_neg, vol_neutral = [0] * 4
            user_total, user_pos, user_neg, user_neutral = [0] * 4
            klout_total, klout_neutral, klout_pos, klout_neg = [0] * 4
            for user in self.scoreCard[candidate]:
                userInWindow = False
                this_user_pos, this_user_neg, this_user_neutral = [0] * 3
                for datestr in self.scoreCard[candidate][user]:
                    date = datetime.strptime(datestr, "%d %b %Y")
                    if date >= fromDate and date <= toDate:
                        userInWindow = True
                        for tuple in self.scoreCard[candidate][user][datestr]:
                            k, s = tuple
                            vol_total += 1
                            if s < 0:
                                vol_neg += 1
                                this_user_neg += 1
                            elif s == 0:
                                vol_neutral += 1
                                this_user_neutral += 1
                            else:
                                vol_pos += 1
                                this_user_pos += 1
                if userInWindow and this_user_pos > this_user_neg and this_user_pos > this_user_neutral:
                    user_pos += 1
                    user_total += 1
                    klout_total += k
                    klout_pos += k
                elif userInWindow and this_user_neg > this_user_pos and this_user_neg > this_user_neutral:
                    user_neg += 1
                    user_total += 1
                    klout_total += k
                    klout_neg += k
                elif userInWindow and this_user_pos == this_user_neg:
                    user_neutral += 1
                    user_total += 1
                    klout_total += k
                    klout_neutral += k

            self.statistics[candidate]["vol_total"] = vol_total
            self.statistics[candidate]["vol_neg"] = vol_neg
            self.statistics[candidate]["vol_pos"] = vol_pos
            self.statistics[candidate]["vol_neutral"] = vol_neutral

            self.statistics[candidate]["user_total"] = user_total
            self.statistics[candidate]["user_neg"] = user_neg
            self.statistics[candidate]["user_pos"] = user_pos
            self.statistics[candidate]["user_neutral"] = user_neutral

            self.statistics[candidate]["klout_total"] = klout_total
            self.statistics[candidate]["klout_neutral"] = klout_neutral
            self.statistics[candidate]["klout_neg"] = klout_neg
            self.statistics[candidate]["klout_pos"] = klout_pos
        return

    def storeStatistics(self, fromDate, toDate):
        self.getStatistics(fromDate, toDate)
        log.info("storing statistics")
        with open(self.scoresFolder + "/" + self.country + "_statistics.csv", "w") as f:
            f.write("Candidate,vol_total,vol_neg,vol_pos,vol_neutral,users_total,users_neg,users_pos,users_neutral\n")
            for candidate in self.statistics:
                f.write(candidate + "," + str(self.statistics[candidate]["vol_total"]) + "," +
                        str(self.statistics[candidate]["vol_neg"]) + "," +
                        str(self.statistics[candidate]["vol_pos"]) + "," +
                        str(self.statistics[candidate]["vol_neutral"]) + "," +
                        str(self.statistics[candidate]["user_total"]) + "," +
                        str(self.statistics[candidate]["user_neg"]) + "," +
                        str(self.statistics[candidate]["user_pos"]) + "," +
                        str(self.statistics[candidate]["user_neutral"]) + "\n")
                log.info("statistics written")
        return

    def getRegressionFeaturesForDateRange(self, fromDate, toDate):
        """ populating the features for regression"""
        self.getStatistics(fromDate, toDate)
        log.debug("inside getRegressionFeaturesForDateRange")

        self.shareOfVolume = {}
        self.shareOfVolume["mentions"] = {}
        self.shareOfVolume["mentions"]["neutral"] = {}
        self.shareOfVolume["mentions"]["positive"] = {}
        self.shareOfVolume["mentions"]["negative"] = {}
        self.shareOfVolume["users"] = {}
        self.shareOfVolume["users"]["neutral"] = {}
        self.shareOfVolume["users"]["positive"] = {}
        self.shareOfVolume["users"]["negative"] = {}
        self.shareOfVolume["klout"] = {}
        self.shareOfVolume["klout"]["neutral"] = {}
        self.shareOfVolume["klout"]["positive"] = {}
        self.shareOfVolume["klout"]["negative"] = {}
        neutral_mentions, pos_mentions, neg_mentions = [1] * 3
        neutral_users, pos_users, neg_users = [1] * 3
        neutral_klout, pos_klout, neg_klout = [1] * 3

        for candidate in self.statistics:
            neutral_mentions += self.statistics[candidate]["vol_neutral"]
            pos_mentions += self.statistics[candidate]["vol_pos"]
            neg_mentions += self.statistics[candidate]["vol_neg"]

            neutral_users += self.statistics[candidate]["user_neutral"]
            pos_users += self.statistics[candidate]["user_pos"]
            neg_users += self.statistics[candidate]["user_neg"]

            neutral_klout += self.statistics[candidate]["klout_neutral"]
            pos_klout += self.statistics[candidate]["klout_pos"]
            neg_klout += self.statistics[candidate]["klout_neg"]

        for candidate in self.statistics:
            self.shareOfVolume["mentions"]["neutral"][candidate] = self.statistics[candidate]["vol_neutral"] / neutral_mentions * 100
            self.shareOfVolume["mentions"]["positive"][candidate] = self.statistics[candidate]["vol_pos"] / pos_mentions * 100
            self.shareOfVolume["mentions"]["negative"][candidate] = self.statistics[candidate]["vol_neg"] / neg_mentions * 100

            self.shareOfVolume["users"]["neutral"][candidate] = self.statistics[candidate]["user_neutral"] / neutral_users * 100
            self.shareOfVolume["users"]["positive"][candidate] = self.statistics[candidate]["user_pos"] / pos_users * 100
            self.shareOfVolume["users"]["negative"][candidate] = self.statistics[candidate]["user_neg"] / neg_users * 100

            self.shareOfVolume["klout"]["neutral"][candidate] = self.statistics[candidate]["klout_neutral"] / neutral_klout * 100
            self.shareOfVolume["klout"]["negative"][candidate] = self.statistics[candidate]["klout_neg"] / neg_klout * 100
            self.shareOfVolume["klout"]["positive"][candidate] = self.statistics[candidate]["klout_pos"] / pos_klout * 100

        self.getShareOfSentiment(fromDate, toDate)

        return

    def getShareOfSentiment(self, fromDate, toDate):
        """scale the overall sentiment of the candidate to [0,1]"""

        log.debug("calculating share(sentiment) for regression feature")
        fromDate = datetime.strptime(fromDate, "%d %b %Y")
        toDate = datetime.strptime(toDate, "%d %b %Y")
        sentiment = {}
        for candidate in self.candidatesJson:
            sentiment[candidate] = 0
            for user in self.scoreCard[candidate]:
                for datestr in self.scoreCard[candidate][user]:
                    date = datetime.strptime(datestr, "%d %b %Y")
                    if date >= fromDate and date <= toDate:
                        for k, s in self.scoreCard[candidate][user][datestr]:
                            sentiment[candidate] += s

        max_sent, min_sent = 0, 0
        for candidate in self.candidatesJson:
            sent = sentiment[candidate]
            if sent < min_sent:
                min_sent = sent
            if sent > max_sent:
                max_sent = sent

        oldMin, oldMax = min_sent, max_sent
        oldRange = oldMax - oldMin
        if oldRange == 0:   # to avoid divide by zero exception
            oldRange = 1
        newMin, newMax = -100, 100
        newRange = newMax - newMin

        self.shareOfVolume["sentiment"] = {}
        for candidate in self.candidatesJson:
            self.shareOfVolume["sentiment"][candidate] = (((sentiment[candidate] - oldMin) * newRange) / oldRange) + newMin
        return

    def normalizeScores(self, scoreList):
        summation = 0
        for candidate in scoreList:
            summation += scoreList[candidate]
        if summation > 100:
            for candidate in scoreList:
                scoreList[candidate] = scoreList[candidate] / summation * 100
        return scoreList

    def getWinner(self, fromDateStr, toDateStr, regressionType):
        winner, winningScore = None, 0
        runnerUp, runnerUpScore = None, 0
        dateList, finalScores = self.getRegressionResults(fromDateStr, toDateStr, regressionType)
        finalScores = self.normalizeScores(finalScores)
        for candidate in finalScores:
            if finalScores[candidate] > winningScore:
                winningScore = finalScores[candidate]
                winner = candidate
        for candidate in finalScores:
            if candidate != winner and finalScores[candidate] >= runnerUpScore:
                runnerUpScore = finalScores[candidate]
                runnerUp = candidate
        return winner, winningScore, runnerUp, runnerUpScore, finalScores

    def getRegressionResults(self, fromDateStr, toDateStr, regressionType='OLS'):
        regressionResults = {}
        fromDate = datetime.strptime(fromDateStr, "%d %b %Y")
        toDate = datetime.strptime(toDateStr, "%d %b %Y")
        # populate regression space for each candidate and get prediction
        for candidate in self.candidatesJson:
            dateList = self.candidatesJson[candidate]["polls"].keys()
            # removing data points that are outside the range
            for dateStr in dateList:
                date = datetime.strptime(dateStr, "%d %b %Y")
                if date <= fromDate or date >= toDate:
                    dateList.remove(dateStr)
            # sorting the data points according to date
            dateList = sorted(dateList, key=lambda x: datetime.strptime(x, "%d %b %Y"))

            Y_train = []  # Y is dependant variable ie list of pollingData sorted by dates
            X_train = []  # X is independent variable feature set
            for dateStr in dateList:
                Y_train.append(self.candidatesJson[candidate]["polls"][dateStr])
                x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11 = [0] * 12  # individual feature vectors
                localFromDateStr, localToDateStr = None, dateStr
                localToDate = datetime.strptime(dateStr, "%d %b %Y")
                localFromDate = localToDate - timedelta(days=10)
                localFromDateStr = localFromDate.strftime("%d %b %Y")
                self.getRegressionFeaturesForDateRange(localFromDateStr, localToDateStr)
                x0 = 1
                x1 = self.shareOfVolume["users"]["neutral"][candidate]
                x2 = self.shareOfVolume["users"]["positive"][candidate]
                x3 = self.shareOfVolume["users"]["negative"][candidate]
                x4 = self.shareOfVolume["mentions"]["neutral"][candidate]
                x5 = self.shareOfVolume["mentions"]["positive"][candidate]
                x6 = self.shareOfVolume["mentions"]["negative"][candidate]
                x7 = self.shareOfVolume["klout"]["neutral"][candidate]
                x8 = self.shareOfVolume["klout"]["positive"][candidate]
                x9 = self.shareOfVolume["klout"]["negative"][candidate]
                x10 = self.shareOfVolume["sentiment"][candidate]
                if self.candidatesJson[candidate]["incumbent"] == "Y":
                    x11 = 1
                X_train.append([x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11])
            log.debug("featureSpace(" + candidate + "-->" + str(X_train))
            # perform the regression fit
            regression = None
            if regressionType == 'LASSO':
                regression = linear_model.Lasso(alpha=0.1, normalize=True)
            elif regressionType == 'OLS':
                regression = linear_model.LinearRegression(normalize=True)
            regression.fit(X_train, Y_train)
            weights = regression.coef_
            log.debug("weights(" + candidate + "-->)" + str(weights) + "\n")
            # make prediction using weights learnt
            X_predict = []
            x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11 = [0] * 12  # individual feature vectors
            self.getRegressionFeaturesForDateRange(fromDateStr, toDateStr)
            x0 = 1
            x1 = self.shareOfVolume["users"]["neutral"][candidate]
            x2 = self.shareOfVolume["users"]["positive"][candidate]
            x3 = self.shareOfVolume["users"]["negative"][candidate]
            x4 = self.shareOfVolume["mentions"]["neutral"][candidate]
            x5 = self.shareOfVolume["mentions"]["positive"][candidate]
            x6 = self.shareOfVolume["mentions"]["negative"][candidate]
            x7 = self.shareOfVolume["klout"]["neutral"][candidate]
            x8 = self.shareOfVolume["klout"]["positive"][candidate]
            x9 = self.shareOfVolume["klout"]["negative"][candidate]
            x10 = self.shareOfVolume["sentiment"][candidate]
            if self.candidatesJson[candidate]["incumbent"] == "Y":
                x11 = 1
            X_predict.append([x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11])
            regressionResults[candidate] = regression.predict(X_predict)[0]
            # handling value out of bounds
            if regressionResults[candidate] < 0:
                regressionResults[candidate] = 0
            if regressionResults[candidate] > 100:
                regressionResults[candidate] = 100
        return dateList, regressionResults

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
            log.debug("processing file %d/%d-->%s" % (fileCount, totalFiles, _file))
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
        surrogate["derivedFrom"]["comments"] = "tweets were filtered by country then state and then by those containing the terms of candidates"
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
        # warning["confidence"] = 1.00
        warning["confidenceIsProbability"] = True
        warning["eventDate"] = self.electionDate
        warning["population"] = winner.title()
        warning["location"] = [self.country.title(), self.state.title(),
                               self.city.title()]
        comment = "Winner: " + winner + " Score: " + str(winningScore) + " Runner-Up: " + runnerUp + " Score: " + str(runnerUpScore)
        if self.runOff == 'Y':
            comment = "Elections going to 2nd round with candidates: " + winner + ' and ' + runnerUp
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
                    default='../data/scores/MX/')
    ap.add_argument('-cf', '--configFile', type=str,
                    help='election configuration file',
                    default='../configFiles/electionConfig_MX')
    ap.add_argument('-d1', '--fromDate', type=str,
                    help='fromDate')
    ap.add_argument('-d2', '--toDate', type=str,
                    help='toDate')
    ap.add_argument('-f1', '--flag1', help="countOrPredict",
                    type=str, default='2')
    ap.add_argument('-r', '--regression', help="regressionType",
                    type=str, default='LASSO')
    ap.add_argument('-f2', '--flag2', help="flag to push surrogates and warning to S3",
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
            winner, winningScore, runnerUp, runnerUpScore, finalScore = elections.getWinner(arg.fromDate, arg.toDate, arg.regression)
            print "------------Regression Results-----------"
            print finalScore
            print winner + "====>" + str(winningScore)
            print "-----------------------------------------"
    except Exception as e:
        log.exception("error while calculating winner:%s", e)

    try:
        elections.createSurrogate(winner, winningScore, runnerUp, runnerUpScore, arg.flag2)
    except Exception as e:
        log.exception("error during creating warnings")

    try:
        if (arg.flag2 == '1'):
            elections.storeStatistics(arg.fromDate, arg.toDate)
    except Exception as e:
        log.exception("error in storing statistics:%s", e)

    log.info("ALL Operations Complete")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print e
