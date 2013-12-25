package edu.umd.cs.linqs.twitter;

import java.io.FileReader
import java.util.concurrent.TimeUnit

import edu.umd.cs.psl.config.*
import edu.umd.cs.psl.database.RDBMS.DatabaseDriver
import edu.umd.cs.psl.groovy.*
import edu.umd.cs.psl.groovy.experiments.ontology.*
import edu.umd.cs.psl.model.predicate.Predicate
import edu.umd.cs.psl.ui.functions.textsimilarity.*
import edu.umd.cs.psl.evaluation.resultui.UIFullInferenceResult
import edu.umd.cs.linqs.twitter.FileAtomPrintStream
import edu.umd.cs.linqs.twitter.SquashingFunction
import edu.umd.cs.linqs.twitter.StringContains

/*
Set up model
 */
ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("twitter")

PSLModel m = new PSLModel(this)

m.add predicate: "TWEETED", user : Entity, tweet: Entity
m.add predicate: "CONTAINS_HASHTAG" , tweet: Entity, hashtag: Entity
m.add predicate: "MENTIONS" , tweet: Entity, user: Entity
m.add predicate: "isMember", user: Entity, grp: Entity, open: true
m.add predicate: "POSITIVE_SENTIMENT", tweet: Entity, sentimentScore: Text
m.add predicate: "NEGATIVE_SENTIMENT", tweet: Entity, sentimentScore: Text
m.add predicate: "SEED_LIKES", hashtag: Entity, grp: Entity
m.add predicate: "groupLikesHashtag", hashtag: Entity, grp: Entity, open: true
m.add predicate: "groupHatesHashtag", hashtag: Entity, grp: Entity, open: true
m.add predicate: "groupLikesGroup", grp1: Entity, grp2: Entity, open: true
m.add predicate: "groupHatesGroup", grp1: Entity, grp2: Entity, open: true


m.add function: "SQUASH", score: Text, implementation: new SquashingFunction(5.0)

m.add rule: (SEED_LIKES(H,G)) >> groupLikesHashtag(H,G), weight: 1.0

m.add rule: (TWEETED(A,T) & CONTAINS_HASHTAG(T,H) & groupLikesHashtag(H,G) & POSITIVE_SENTIMENT(T,S) & SQUASH(S)) >> isMember(A,G), weight: 1.0
m.add rule: (TWEETED(A,T) & CONTAINS_HASHTAG(T,H) & groupHatesHashtag(H,G) & NEGATIVE_SENTIMENT(T,S) & SQUASH(S)) >> isMember(A,G), weight: 1.0

// propagate hashtag preferences
m.add rule: (isMember(A,G) & TWEETED(A,T) & CONTAINS_HASHTAG(T,H) & POSITIVE_SENTIMENT(T,S) & SQUASH(S)) >> groupLikesHashtag(H,G), weight: 1.0
m.add rule: (isMember(A,G) & TWEETED(A,T) & CONTAINS_HASHTAG(T,H) & NEGATIVE_SENTIMENT(T,S) & SQUASH(S)) >> groupHatesHashtag(H,G), weight: 1.0

// propagate through social graph
m.add rule: (isMember(A,G) & TWEETED(B,T) & MENTIONS(T,A) & POSITIVE_SENTIMENT(T,S) & SQUASH(S)) >> isMember(B,G), weight: 1.0
m.add rule: (isMember(A,G) & TWEETED(A,T) & MENTIONS(T,B) & POSITIVE_SENTIMENT(T,S) & SQUASH(S)) >> isMember(B,G), weight: 1.0

m.add rule: groupHatesHashtag(H,G) >> ~groupLikesHashtag(H,G), constraint: true

// inter group relations

m.add rule: (isMember(A,G1) & TWEETED(B,T) & MENTIONS(T,A) & POSITIVE_SENTIMENT(T,S) & SQUASH(S) & isMember(B,G2) & (G1 - G2)) >> groupLikesGroup(G1,G2), weight: 1.0
m.add rule: (isMember(A,G1) & TWEETED(A,T) & MENTIONS(T,B) & POSITIVE_SENTIMENT(T,S) & SQUASH(S) & isMember(B,G2) & (G1 - G2)) >> groupLikesGroup(G1,G2), weight: 1.0
m.add rule: (isMember(A,G1) & TWEETED(B,T) & MENTIONS(T,A) & NEGATIVE_SENTIMENT(T,S) & SQUASH(S) & isMember(B,G2) & (G1 - G2)) >> groupHatesGroup(G1,G2), weight: 1.0
m.add rule: (isMember(A,G1) & TWEETED(A,T) & MENTIONS(T,B) & NEGATIVE_SENTIMENT(T,S) & SQUASH(S) & isMember(B,G2) & (G1 - G2)) >> groupHatesGroup(G1,G2), weight: 1.0

m.add rule: (groupLikesGroup(G1,G2) & groupLikesHashtag(H, G1)) >> groupLikesHashtag(H, G2), weight: 1.0
m.add rule: (groupLikesGroup(G1,G2) & groupHatesHashtag(H, G1)) >> groupHatesHashtag(H, G2), weight: 1.0
m.add rule: (groupHatesGroup(G1,G2) & groupLikesHashtag(H, G1)) >> groupHatesHashtag(H, G2), weight: 1.0
m.add rule: (groupHatesGroup(G1,G2) & groupHatesHashtag(H, G1)) >> groupLikesHashtag(H, G2), weight: 1.0

m.add rule: (groupLikesGroup(G1,G2) & groupLikesGroup(G2,G3)) >> groupLikesGroup(G1,G3), weight: 1.0
m.add rule: (groupLikesGroup(G1,G2) & groupHatesGroup(G2,G3)) >> groupHatesGroup(G1,G3), weight: 1.0
m.add rule: (groupHatesGroup(G1,G2) & groupHatesGroup(G2,G3)) >> groupLikesGroup(G1,G3), weight: 1.0

m.add rule: groupHatesGroup(H,G) >> ~groupLikesGroup(H,G), constraint: true



// 
m.add rule: (groupHatesGroup(G1,G2) & isMember(U,G1)) >> ~isMember(U,G2), weight: 1.0
m.add rule: (groupLikesGroup(G1,G2) & isMember(U,G1)) >> isMember(U,G2), weight: 1.0

m.add rule: (isMember(U,G1) & isMember(U,G2) & (G1-G2)) >> groupLikesGroup(G1,G2), weight: 1.0



m.add PredicateConstraint.PartialFunctional, on: isMember
m.add Prior.Simple, on: groupLikesHashtag, weight: 0.01
m.add Prior.Simple, on: groupHatesHashtag, weight: 0.01
m.add Prior.Simple, on: groupLikesGroup, weight: 0.01
m.add Prior.Simple, on: groupHatesGroup, weight: 0.01
m.add Prior.Simple, on: isMember, weight: 0.01

/***
Load data
**/

DataStore data = new RelationalDataStore([entityid:'string'], m)
data.setup db: DatabaseDriver.H2, type: "memory"
//data.setup db: DatabaseDriver.H2, folder: "/tmp/"


def insert
/* 
 * We start by reading in the non-target (i.e. evidence) predicate data.
 */

dataDir = "election/election_predicates"

insert = data.getInserter(TWEETED,0)
insert.loadFromFile(dataDir + "/tweeted.txt")
insert = data.getInserter(MENTIONS,0)
insert.loadFromFile(dataDir + "/mentions.txt")
insert = data.getInserter(CONTAINS_HASHTAG,0)
insert.loadFromFile(dataDir + "/contains_hashtag.txt")
insert = data.getInserter(POSITIVE_SENTIMENT,0)
insert.loadFromFile(dataDir + "/positive_sentiment.txt")
insert = data.getInserter(NEGATIVE_SENTIMENT,0)
insert.loadFromFile(dataDir + "/negative_sentiment.txt")


insert = data.getInserter(isMember, 0)

//for (int i = 0; i < 5; i++) 
//	insert.loadFromFile("lda/seed" + i + ".txt")
for (int i = 0; i < 10; i++) 
	insert.loadFromFile("election_lda_seeds/10seed" + i + ".txt")

//insert = data.getInserter(groupLikesHashtag, 0)
//insert.insert("#elmundoconchavez", "Chavez_Supporters")
//insert.insert("#hayuncamino", "Capriles_Supporters")

//println "Count: " + data.getDatabase(read: 0).query((tweeted(A,T) & containsHashtag(T,H) & groupLikesHashtag(H,G) & isPositive(T)).getFormula()).size()

/*** INFERENCE ***/

println "Running inference"

def inference = m.mapInference(data.getDatabase(read: 0), cb)

def writer 

writer = new FileAtomPrintStream("isMemberElectionLDA.txt")
inference.printAtoms(isMember, writer, false)

writer = new FileAtomPrintStream("groupLikesHashtagElectionLDA.txt")
inference.printAtoms(groupLikesHashtag, writer, false)
writer = new FileAtomPrintStream("groupHatesHashtagElectionLDA.txt")
inference.printAtoms(groupHatesHashtag, writer, false)
writer = new FileAtomPrintStream("groupLikesGroupElectionLDA.txt")
inference.printAtoms(groupLikesGroup, writer, false)
writer = new FileAtomPrintStream("groupHatesGroupElectionLDA.txt")
inference.printAtoms(groupHatesGroup, writer, false)


