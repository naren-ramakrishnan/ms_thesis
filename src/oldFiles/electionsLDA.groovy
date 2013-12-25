/*  Author = Aravindan Mahendiran
    email  = aravind@vt.edu
    version = 1.0
    comments = Bert's code Replica
*/

package edu.umd.cs.linqs.embers

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.Reader

import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang.Math

import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.ReadOnlyDatabase
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type

import edu.umd.cs.psl.groovy.PSLModel
import edu.umd.cs.psl.groovy.PredicateConstraint

import edu.umd.cs.psl.model.Model
import edu.umd.cs.psl.model.argument.Attribute
import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.argument.StringAttribute
import edu.umd.cs.psl.model.argument.UniqueID
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.model.atom.RandomVariableAtom
import edu.umd.cs.psl.model.predicate.Predicate
import edu.umd.cs.psl.model.predicate.PredicateFactory
import edu.umd.cs.psl.model.function.ExternalFunction

import edu.umd.cs.psl.parser.PSLModelLoader
import edu.umd.cs.psl.util.database.Queries

import edu.umd.cs.psl.application.inference.LazyMPEInference

class electionsLDA{

    // Set up model
	private DataStore data
	private final ConfigBundle cb

	private final Logger log = LoggerFactory.getLogger(this.class)
	private final ConfigManager cm = ConfigManager.getManager()

	private final String CONFIG_PREFIX = "electionsLDA"
	/* Key for model filename */
	private final String MODEL_FILENAME = CONFIG_PREFIX + ".model"
    private final String defaultPath = '/hdd/psl/'
	private final String BLANK = "-"

	private Partition observedPartition
	private Partition inferredPartition

	private PSLModel model

	public electionsLDA(String inputFolder){
		cb = cm.getBundle(CONFIG_PREFIX)

		String dbPath = cb.getString("dbpath", defaultPath)
		String dbName = cb.getString("dbname", "electionPSLDB")
		String fullDBPath = dbPath + dbName

		data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, true), cb)

		model = new PSLModel(this, data)

        println "building predicates"

		model.add predicate: "TWEETED", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "MENTIONS", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "CONTAINS", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "IS_MEMBER", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "POSITIVE_SENTIMENT", types: [ArgumentType.UniqueID, ArgumentType.String]
		model.add predicate: "NEGATIVE_SENTIMENT", types: [ArgumentType.UniqueID, ArgumentType.String]
        model.add predicate: "SEED_WORD", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "LIKES", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "HATES", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
        model.add predicate: "SQUASH", types: [ArgumentType.String]

        println "adding constraints"
		model.add rule: HATES(W,G) >> ~LIKES(W,G), constraint: true
        //model.add PredicateConstraint.PartialFunctional, on: LIKES
        //model.add PredicateConstraint.PartialFunctional, on: HATES
        model.add PredicateConstraint.PartialFunctional, on: IS_MEMBER

        println "adding priors"
        model.add rule: ~LIKES(W,G), weight: 0.01
		model.add rule: ~HATES(W,G), weight: 0.01
        model.add rule: ~IS_MEMBER(U,G), weight: 0.01

        println"defining rules"
        model.add rule: SEED_WORD(W,G) >> LIKES(W,G), weight: 1.0

        //membership rules
		model.add rule: (TWEETED(A,T) & CONTAINS(T,W) & LIKES(W,G) & POSITIVE_SENTIMENT(T,S) & SQUASH(S)) >> IS_MEMBER(A,G), weight: 1.0
		model.add rule: (TWEETED(A,T) & CONTAINS(T,W) & HATES(W,G) & NEGATIVE_SENTIMENT(T,S) & SQUASH(S)) >> IS_MEMBER(A,G), weight: 1.0

        //propagate word preferences
		model.add rule: (IS_MEMBER(A,G) & TWEETED(A,T) & CONTAINS(T,W) & POSITIVE_SENTIMENT(T,S) & SQUASH(S)) >> LIKES(W,G), weight: 1.0
		model.add rule: (IS_MEMBER(A,G) & TWEETED(A,T) & CONTAINS(T,W) & NEGATIVE_SENTIMENT(T,S) & SQUASH(S)) >> HATES(W,G), weight: 1.0

        // propagate membership through social graph
		model.add rule: (IS_MEMBER(A,G) & TWEETED(B,T) & MENTIONS(T,A) & POSITIVE_SENTIMENT(T,S) & SQUASH(S)) >> IS_MEMBER(B,G), weight: 1.0
		model.add rule: (IS_MEMBER(A,G) & TWEETED(A,T) & MENTIONS(T,B) & POSITIVE_SENTIMENT(T,S) & SQUASH(S)) >> IS_MEMBER(B,G), weight: 1.0

		observedPartition = new Partition(cb.getInt("partitions.trainread", -1))
        inferredPartition = new Partition(cb.getInt("partitions.trainwrite", -1))

		loadObservedData(inputFolder, observedPartition)
	}

	private void loadObservedData(String inputFolder, Partition observedPartition){
		String tweetedFile = inputFolder + '/tweeted.csv'
        String mentionsFile = inputFolder + '/mentioned.csv'
        String containsWordsFile = inputFolder + '/containsWord.csv'
        String sentimentsFile = inputFolder + '/sentiment.csv'
        String seedWordsFile = inputFolder + '/seedWords.csv'

        def readDB = data.getDatabase(observedPartition)

        //insert TWEETED predicates
        loadTweetedPredicate(tweetedFile, readDB)

        //insert CONTAINS predicates
        loadContainsWordPredicate(containsWordsFile, readDB)

        //insert MENTIONS predicates
        loadMentionsPredicate(mentionsFile, readDB)

        //insert SENTIMENT predicates
        loadSentimentPredicate(sentimentsFile, readDB)

        //insert SEED_WORD predicates
        loadSeedWordPredicate(seedWordsFile, readDB)

        //insert SQUASH predicate
        loadSquashPredicate(readDB)


		readDB.close()
	}

    public void loadSquashPredicate(Database db){
        int minSent = 0
        int maxSent = 15
        double coeff = 5.0

        for (int sent=minSent; sent<=maxSent; sent++){
            double truthValue = (1 - Math.exp(-coeff * sent))
            String sentStr = Double.toString(Math.abs(sent))
            insertAtom(truthValue, db, SQUASH, sentStr)
        }

        println "SQUASH predicate loaded"
    }

    private void loadTweetedPredicate(String inputFile, Database db){
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while(reader.ready()){
            String line = reader.readLine()
            String[] splits = line.split(',')
            insertAtom(1.0, db, TWEETED, splits[0], splits[1])
        }
        reader.close()
        decoder.close()
        fis.close()
        println "TWEETED predicate loaded"
    }

	private void loadContainsWordPredicate(String inputFile, Database db){
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while(reader.ready()){
            String line = reader.readLine()
            String[] splits = line.split(',')
            insertAtom(1.0, db, CONTAINS, splits[0], splits[1])
        }
        reader.close()
        decoder.close()
        fis.close()
        println "CONTAINS predicate loaded"
   }

    private void loadMentionsPredicate(String inputFile, Database db){
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while(reader.ready()){
            String line = reader.readLine()
            String[] splits = line.split(',')
            insertAtom(1.0, db, MENTIONS, splits[0], splits[1])
        }
        reader.close()
        decoder.close()
        fis.close()
        println "MENTIONS predicate loaded"
    }

    private void loadSentimentPredicate(String inputFile, Database db){
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)

        while (reader.ready()){
            String line = reader.readLine()
            String[] splits = line.split(',')
            int sentiment = Integer.parseInt(splits[1])
            String sentimentString = Integer.toString(Math.abs(sentiment))

            if(sentiment == 0){
                insertAtom(0.5, db, POSITIVE_SENTIMENT, splits[0], sentimentString)
                insertAtom(0.5, db, NEGATIVE_SENTIMENT, splits[0], sentimentString)
            }
            else if(sentiment > 0){
                insertAtom(1.0, db, POSITIVE_SENTIMENT, splits[0], sentimentString)
            }
            else{
                insertAtom(1.0, db, NEGATIVE_SENTIMENT, splits[0], sentimentString)
            }
        }
        reader.close()
        decoder.close()
        fis.close()
        println "SENTIMENT predicate loaded"
   }

    private void loadSeedWordPredicate(String inputFile, Database db){
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while (reader.ready()){
            String line = reader.readLine()
            String[] splits = line.split(',')
            insertAtom(1.0, db, SEED_WORD, splits[0], splits[1])
        }
        reader.close()
        decoder.close()
        fis.close()
        println "SEED_WORD predicate loaded"
    }

	private void insertAtom(Double value, Database db, Predicate pred, String ... args){
		GroundTerm [] convertedArgs = Queries.convertArguments(db, pred, args)
		RandomVariableAtom atom = db.getAtom(pred, convertedArgs)
		atom.setValue(value)
		atom.commitToDB()
	}

    public void inferAndPrint(String outputFolder){
        println "running inference"
        Database db = data.getDatabase(inferredPartition, [TWEETED, MENTIONS, CONTAINS, POSITIVE_SENTIMENT, NEGATIVE_SENTIMENT, SEED_WORD, SQUASH] as Set, observedPartition)
        LazyMPEInference inferenceApp = new LazyMPEInference(model,db, cb)
        inferenceApp.mpeInference()
        println "inference complete ...printing output"

        String memberFile = outputFolder + '/membership.tsv'
        String likesFile = outputFolder + '/likes.tsv'
        String hatesFile = outputFolder + '/hates.tsv'
        BufferedWriter writer = null

        //writing the IS_MEMBER predicate to the file
        writer = new BufferedWriter(new FileWriter(memberFile))
        for(GroundAtom atom: Queries.getAllAtoms(db,IS_MEMBER)){
            String line = atom.toString() + "\t" + atom.getValue() + "\n"
            writer.write(line)
        }

        println "IS_MEMBER predicate dumped"

        //writing the LIKES predicate to the file
        writer = new BufferedWriter(new FileWriter(likesFile))
        for(GroundAtom atom: Queries.getAllAtoms(db,LIKES)){
            String line = atom.toString() + "\t" + atom.getValue() + "\n"
            writer.write(line)
        }

        println "LIKES predicate dumped"
        //writing the HATES predicate to the file
        writer = new BufferedWriter(new FileWriter(hatesFile))
        for(GroundAtom atom: Queries.getAllAtoms(db,HATES)){
            String line = atom.toString() + "\t" + atom.getValue() + "\n"
            writer.write(line)
        }

        println "HATES predicate dumped"
        writer.close()
    }

    public static void main(String [] args){
		def inputFolder
        def outputFolder

        if (args.length < 2){
            println "input and outputFolder not used hence using defaults"
            inputFolder = '/home/aravindan/Dropbox/git/ms_thesis/data/psl/venezuela/01Oct/inputs'
            outputFolder ='/home/aravindan/Dropbox/git/ms_thesis/data/psl/venezuela/01Oct/outputs'
        }
        else{
            inputFolder = args[0]
            outputFolder = args[1]
        }

        def election = new electionsLDA(inputFolder)
        println "All predicates loaded"
		election.inferAndPrint(outputFolder)
	}
}
