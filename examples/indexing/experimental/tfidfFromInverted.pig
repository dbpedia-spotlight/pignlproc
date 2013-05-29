/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 *      - use Pig/hadoop to generate an inverted index
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

SET default_parallel 12;

SET job.name 'tfidf-index-from-Inverted Index for $LANG';
--SET mapred.compress.map.output 'true';
--SET mapred.map.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';

inverted = LOAD '$INVERTED_INDEX' USING PigStorage('\t', '-schema');

flattened = FOREACH inverted GENERATE 
	token,
	FLATTEN(sorted) AS (uri, weight);

DESCRIBE flattened;

grouped = GROUP flattened BY uri;

raw_tfidf = FOREACH grouped {  
	filtered = FILTER flattened BY (SIZE(token) > 1);
	GENERATE
	group AS uri,
	filtered.(token, weight) AS tokens;
};

tfidf = FOREACH raw_tfidf {
	sorted = ORDER tokens BY weight desc;
	GENERATE uri, sorted;
};

STORE tfidf INTO '$OUTPUT_DIR/$LANG.tfidf.tsv.bz2' USING PigStorage('\t', '-schema');

 
