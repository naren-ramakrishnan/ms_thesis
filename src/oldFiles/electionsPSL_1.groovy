/*	Author 		= 	Aravindan Mahendiran
 * 	email 		= 	aravind@vt.edu
 * 	version		=	1.0.0
 * 	comments	=	basic propagation like in Bert's paper
 *                  seed_words & belonged_to ==> belongs_to
 *                  propagation based on belongs_to and is_member
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

class electionsPSL
{
    // Set up model
	private DataStore data
	private final ConfigBundle cb

	private final Logger log = LoggerFactory.getLogger(this.class)
	private final ConfigManager cm = ConfigManager.getManager()

	private final String CONFIG_PREFIX = "electionPSL"
	/* Key for model filename */
	private final String MODEL_FILENAME = CONFIG_PREFIX + ".model"
    private final String defaultPath = '/hdd/psl/'
	private final String BLANK = "-"

	private Partition observedPartition
	private Partition inferredPartition

	private PSLModel model

	public electionsPSL(String inputFolder)
    {
		cb = cm.getBundle(CONFIG_PREFIX)

		String dbPath = cb.getString("dbpath", defaultPath)
		String dbName = cb.getString("dbname", "electionPSLDB")
		String fullDBPath = dbPath + dbName

		data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, fullDBPath, true), cb)

		model = new PSLModel(this, data)

        log.info ("building predicates")
		model.add predicate: "TWEETED", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "MENTIONS", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "RETWEET_OF", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "CONTAINS", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "IS_MEMBER", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "WAS_MEMBER", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
        model.add predicate: "POSITIVE_SENTIMENT", types: [ArgumentType.UniqueID]
		model.add predicate: "NEGATIVE_SENTIMENT", types: [ArgumentType.UniqueID]
		model.add predicate: "SEED_WORD", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
		model.add predicate: "BELONGS_TO", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]
        model.add predicate: "BELONGED_TO", types: [ArgumentType.UniqueID, ArgumentType.UniqueID]

        log.info("adding constraints")
		model.add PredicateConstraint.PartialFunctional, on: IS_MEMBER
        model.add PredicateConstraint.PartialFunctional, on: BELONGS_TO

        log.info("adding priors")
        model.add rule: ~BELONGS_TO(W,G), weight: 0.01
        model.add rule: ~IS_MEMBER(U,G), weight: 0.01

        log.info("defining rules")
        model.add rule: BELONGED_TO(W,G) >> BELONGS_TO(W,G), weight: 1.0
        model.add rule: WAS_MEMBER(U,G) >> IS_MEMBER(U,G), weight: 2.0
        model.add rule: SEED_WORD(W,G) >> BELONGS_TO(W,G), weight: 2.0

        //membership rules
		model.add rule: (TWEETED(A,T) & CONTAINS(T,W) & BELONGS_TO(W,G) & POSITIVE_SENTIMENT(T)) >> IS_MEMBER(A,G), weight: 1.0
        model.add rule: (TWEETED(A,T) & CONTAINS(T,W) & BELONGS_TO(W,G) & NEGATIVE_SENTIMENT(T)) >> ~IS_MEMBER(A,G), weight: 1.0

        //propagating membership through social graph
        //retweets
        model.add rule: (IS_MEMBER(B,G) & TWEETED(A,T) & RETWEET_OF(T,B)) >> IS_MEMBER(A,G), weight: 1.0
        model.add rule: (IS_MEMBER(B,G) & TWEETED(B,T) & RETWEET_OF(T,A)) >> IS_MEMBER(A,G), weight: 1.0
        //mentions
        model.add rule: (IS_MEMBER(A,G) & TWEETED(B,T) & MENTIONS(T,A) & POSITIVE_SENTIMENT(T)) >> IS_MEMBER(B,G), weight: 1.0
		model.add rule: (IS_MEMBER(A,G) & TWEETED(A,T) & MENTIONS(T,B) & POSITIVE_SENTIMENT(T)) >> IS_MEMBER(B,G), weight: 1.0
        //propagate word preferences
        model. add rule: (IS_MEMBER(A,G) & TWEETED(A,T) & CONTAINS(T,W) & POSITIVE_SENTIMENT(T)) >> BELONGS_TO(W,G), weight: 2.0
        model. add rule: (IS_MEMBER(A,G) & TWEETED(A,T) & CONTAINS(T,W) & NEGATIVE_SENTIMENT(T)) >> ~BELONGS_TO(W,G), weight: 2.0

		//load data for inference
		observedPartition = new Partition(cb.getInt("partitions.trainread", -1))
        inferredPartition = new Partition(cb.getInt("partitions.trainwrite", -1))

		loadObservedData(inputFolder, observedPartition)
	}

	private void loadObservedData(String inputFolder, Partition observedPartition)
    {
		String tweetedFile = inputFolder + '/tweeted.csv'
        String mentionsFile = inputFolder + '/mentioned.csv'
        String retweetFile = inputFolder + '/retweet.csv'
        String containsWordsFile = inputFolder + '/containsWord.csv'
        String sentimentsFile = inputFolder + '/sentiment.csv'
        String seedWordsFile = inputFolder + '/seedWords.csv'
        String belongedToFile = inputFolder + '/belongedTo.csv'
        String wasMemberFile = inputFolder + '/wasMember.csv'

        def readDB = data.getDatabase(observedPartition)

        //insert TWEETED predicates
        loadTweetedPredicate(tweetedFile, readDB)

        //insert CONTAINS predicates
        loadContainsWordPredicate(containsWordsFile, readDB)

        //insert MENTIONS predicates
        loadMentionsPredicate(mentionsFile, readDB)

        //insert RETWEET_OF predicates
        loadRetweetPredicate(retweetFile, readDB)

        //insert SENTIMENT predicates
        loadSentimentPredicate(sentimentsFile, readDB)

        //insert SEED_WORDS predicates
        loadSeedWordsPredicate(seedWordsFile, readDB)

        //insert BELONGED_TO predicate
        loadBelongedToPredicate(belongedToFile, readDB)

        //insert WAS_MEMBER predicates
        loadWasMemberPredicate(wasMemberFile, readDB)

		readDB.close()
	}

    private void loadTweetedPredicate(String inputFile, Database db)
    {
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while(reader.ready())
        {
            String line = reader.readLine()
            String[] splits = line.split(',')
            insertAtom(1.0, db, TWEETED, splits[0], splits[1])
        }
        reader.close()
        decoder.close()
        fis.close()
        log.info("TWEETED predicate loaded")
   }

    private void loadContainsWordPredicate(String inputFile, Database db)
    {
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while(reader.ready())
        {
            String line = reader.readLine()
            String[] splits = line.split(',')
            insertAtom(1.0, db, CONTAINS, splits[0], splits[1])
        }
        reader.close()
        decoder.close()
        fis.close()
        log.info("CONTAINS predicate loaded")
   }

    private void loadMentionsPredicate(String inputFile, Database db)
    {
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while(reader.ready())
        {
            String line = reader.readLine()
            String[] splits = line.split(',')
            insertAtom(1.0, db, MENTIONS, splits[0], splits[1])
        }
        reader.close()
        decoder.close()
        fis.close()
        log.info("MENTIONS predicate loaded")
    }

	private void loadRetweetPredicate(String inputFile, Database db)
    {
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while(reader.ready())
        {
            String line = reader.readLine()
            String[] splits = line.split(',')
            insertAtom(1.0, db, RETWEET_OF, splits[0], splits[1])
        }
        reader.close()
        decoder.close()
        fis.close()
        log.info("RETWEET_OF predicate loaded")
    }

    private void loadSentimentPredicate(String inputFile, Database db)
    {
        FileInputStream fis = new FileInputStream(inputFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
        while (reader.ready())
        {
            String line = reader.readLine()
            String[] splits = line.split(',')
            int sentiment = Integer.parseInt(splits[1])
            double coeff = 5.0
            double truthValue = (1 - Math.exp(-coeff * Math.abs(sentiment)))

            if (sentiment == 0)
            {
                insertAtom(0.5, db, POSITIVE_SENTIMENT, splits[0])
                insertAtom(0.5, db, NEGATIVE_SENTIMENT, splits[0])
            }
            else if(sentiment > 0)
				insertAtom(truthValue, db, POSITIVE_SENTIMENT, splits[0])
            else
                insertAtom(truthValue, db, NEGATIVE_SENTIMENT, splits[0])
		}
        reader.close()
        decoder.close()
        fis.close()

        log.info("SENTIMENT predicates loaded")
   }

	private void loadSeedWordsPredicate(String seedWordsFile, Database db)
	{
        FileInputStream fis = new FileInputStream(seedWordsFile)
		Reader decoder = new InputStreamReader(fis, "UTF-8")
		BufferedReader reader = new BufferedReader(decoder)
		while(reader.ready())
		{
			String line = reader.readLine()
			String[] splits = line.split(',')
			double weight = Double.parseDouble(splits[2])
			insertAtom(1.0, db, SEED_WORD, splits[0], splits[1])
		}
		log.info("SEED_WORD predicate loaded")
		reader.close()
		decoder.close()
		fis.close()
    }

    private void loadBelongedToPredicate(String belongedToFile, Database db)
    {
        try
        {
            FileInputStream fis = new FileInputStream(belongedToFile)
		    Reader decoder = new InputStreamReader(fis, "UTF-8")
		    BufferedReader reader = new BufferedReader(decoder)
            while(reader.ready())
            {
                String line = reader.readLine()
                String[] splits = line.split(',')
                double weight = Double.parseDouble(splits[2])
                insertAtom(weight, db, BELONGED_TO, splits[0], splits[1])
            }
            log.info("BELONGED_TO words loaded")
            reader.close()
            decoder.close()
            fis.close()
        }
        catch(FileNotFoundException e)
        {
            log.info("belongedToFile not present..assuming 1st iteration")

        }
	}

	private void loadWasMemberPredicate(String inputFile, Database db)
    {
        try
        {
            FileInputStream fis = new FileInputStream(inputFile)
            Reader decoder = new InputStreamReader(fis, "UTF-8")
		    BufferedReader reader = new BufferedReader(decoder)
            while(reader.ready())
            {
                String line = reader.readLine()
                String[] splits = line.split(',')
                double weight = Double.parseDouble(splits[2])
                insertAtom(weight, db, WAS_MEMBER, splits[0], splits[1])
            }
            log.info("WAS_MEMBER predicate loaded")
            reader.close()
            decoder.close()
            fis.close()
        }
        catch(FileNotFoundException e)
        {
            log.info("wasMemberFile file not present..assuming 1st iteration")
        }
    }

    private void insertAtom(Double value, Database db, Predicate pred, String ... args)
    {
		GroundTerm [] convertedArgs = Queries.convertArguments(db, pred, args)
		RandomVariableAtom atom = db.getAtom(pred, convertedArgs)
		atom.setValue(value)
		atom.commitToDB()
	}

    public void inferAndPrint(String outputFolder)
    {
        log.info("running inference")
        //inference
        Database db = data.getDatabase(inferredPartition, [TWEETED, MENTIONS, RETWEET_OF, CONTAINS, POSITIVE_SENTIMENT, NEGATIVE_SENTIMENT, WAS_MEMBER, BELONGED_TO, SEED_WORD] as Set, observedPartition)
        LazyMPEInference inferenceApp = new LazyMPEInference(model,db, cb)
        inferenceApp.mpeInference()
        log.info("inference complete ...printing output")

        //output
        String memberFile = outputFolder + '/membership.tsv'
        String likesFile = outputFolder + '/belongsTo.tsv'

        BufferedWriter writer = null

        //writing the IS_MEMBER predicate to the file
        writer = new BufferedWriter(new FileWriter(memberFile))
        for(GroundAtom atom: Queries.getAllAtoms(db, IS_MEMBER))
        {
            String line = atom.toString() + "\t" + atom.getValue() + "\n"
            writer.write(line)
        }

        log.info("IS_MEMBER predicate dumped")

        //writing the BELONGS_TO predicate to the file
        writer = new BufferedWriter(new FileWriter(likesFile))
        for(GroundAtom atom: Queries.getAllAtoms(db, BELONGS_TO))
        {
            String line = atom.toString() + "\t" + atom.getValue() + "\n"
            writer.write(line)
        }

        writer.close()
    }

    public static void main(String [] args)
    {
		def inputFolder
        def outputFolder

        if (args.length < 2)
        {
            inputFolder = '/home/aravindan/Dropbox/git/ms_thesis/data/psl/venezuela/01Oct/inputs'
            outputFolder ='/home/aravindan/Dropbox/git/ms_thesis/data/psl/venezuela/01Oct/outputs'
        }
        else
        {
            inputFolder = args[0]
            outputFolder = args[1]
        }

        def election = new electionsPSL(inputFolder)
		election.inferAndPrint(outputFolder)
	}
}

