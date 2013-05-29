/*
 * This script loads an index, then uses a UDF to calculate 
 * cosine Similarity
 * Schema for Loader: URI {(tok1, score1), (tok2, score2)...}
 * Author: Chris Hokamp
 * @params $OUTPUT_DIR - the directory where the files should be stored
 *         $STOPLIST_PATH - the location of the stoplist in HDFS                
 *         $STOPLIST_NAME - the filename of the stoplist
 *         $INPUT - the wikipedia XML dump
 *         $MIN_COUNT - the minumum count for a token to be included in the index
 *         $PIGNLPROC_JAR - the location of the pignlproc jar
 *         $LANG - the language of the Wikidump 
 *         $MAX_SPAN_LENGTH - the maximum length for a paragraph span
 *         $NUM_DOCS - the number of documents in this wikipedia dump
 *         $N - the number of tokens to keep
 *         - use the file 'indexer.pig.params' to supply a default configuration
 */


-- TEST: set parallelism level for reducers
SET default_parallel 15;

SET job.name 'Pairwise Cossine similarity';
--SET mapred.compress.map.output 'true';
--SET mapred.map.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Working Notes
-- It doesn't make sense to do this in Map-reduce
-- see DBpedia-Spotlight Java and Scala code for TF-IDF implementations
-- the only step that should/could be done in Pig is filtering the index to contain only relevant URIs
-- the complexity of the join is O(N!/(r!(n-r)!)) - for pairs, this reduces to SUM(1-(N-1)) 
-- Note: every feature is mirrored, but the values are the same
--	so... this is an "N choose R" problem
