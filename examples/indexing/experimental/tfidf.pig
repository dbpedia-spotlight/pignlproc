/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 *	- use Pig/hadoop to generate a tfidf index for the wikipedia corpus
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

SET job.name 'Wikipedia-Token-Counts-per-URI for $LANG';
--SET mapred.compress.map.output 'true';
--SET mapred.map.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define aliases
DEFINE getTokens pignlproc.index.LuceneTokenizer('$STOPLIST_PATH', '$STOPLIST_NAME', '$LANG', '$ANALYZER_NAME');
--Comment above and uncomment below to use default stoplist for the analyzer
--DEFINE getTokens pignlproc.index.LuceneTokenizer('$LANG', '$ANALYZER_NAME');
DEFINE textWithLink pignlproc.evaluation.ParagraphsWithLink('$MAX_SPAN_LENGTH');
DEFINE JsonCompressedStorage pignlproc.storage.JsonCompressedStorage();
DEFINE keepTopN pignlproc.helpers.FirstNtuples('$N');

-- Parse the wikipedia dump and extract text and links data
parsed = LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('$LANG')
  AS (title, id, pageUrl, text, redirect, links, headers, paragraphs);

-- filter as early as possible
SPLIT parsed INTO
  parsedRedirects IF redirect IS NOT NULL,
  parsedNonRedirects IF redirect IS NULL;

-- Project articles
articles = FOREACH parsedNonRedirects GENERATE
  pageUrl,
  text,
  links,
  paragraphs;

-- the next relation is created in order to get the global doc count
grouped = GROUP articles ALL;

-- Extract paragraph contexts of the links 
paragraphs = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(textWithLink(text, links, paragraphs))
  AS (paragraphIdx, paragraph, targetUri, startPos, endPos);

-----------------
-- BEGIN TFIDF --
-----------------
doc_context = FOREACH paragraphs GENERATE
	REGEX_EXTRACT(targetUri, '(.*)/(.*)', 2) AS uri,
        getTokens(paragraph) AS context;

all_contexts = GROUP doc_context by uri;

--added relation here to filter by number of contexts
size_filter = FILTER all_contexts BY
		COUNT(doc_context) >= 1;

flattened_context = FOREACH size_filter {
	contexts = doc_context.context;
 	GENERATE
	group AS uri,
	FLATTEN(contexts) AS context; 
};

uri_and_token = FOREACH flattened_context GENERATE
	uri,
	FLATTEN(context) AS token;

-- (2) group by token and Unique Doc to get global doc frequency
unique = DISTINCT uri_and_token;

docs_by_tokens = group unique by token;

raw_doc_freq = FOREACH docs_by_tokens GENERATE
	group AS token,
	COUNT(unique) as df;

doc_freq = FILTER raw_doc_freq BY df > $MIN_COUNT;

--NUM_DOCS should be the total number of RESOURCES
raw_idf = foreach doc_freq {
	numDocs = COUNT(grouped.articles);	
        GENERATE
	token,
	LOG((double)numDocs/(double)df) AS idf: double;
};

idf = FILTER raw_idf BY (idf > 0);

--(3) Term Frequency
term_freq = GROUP uri_and_token by (uri, token);
raw_term_counts = FOREACH term_freq GENERATE
	group.uri as uri,
	group.token as token,
	COUNT(uri_and_token) as tf;

term_counts = FILTER raw_term_counts BY tf > $MIN_COUNT;

--(4) put the data together
token_instances = JOIN term_counts BY token, idf by token; 

--(5) calculate tfidf using $NUM_DOCS - note that the user must know how many RESOURCES there are, not how many docs
tfidf = FOREACH token_instances {
	tf_idf = (double)term_counts::tf*(double)idf::idf;
		GENERATE 
			term_counts::uri as uri,
			term_counts::token as token,
			tf_idf as weight;
	};
by_docs = group tfidf BY uri;
docs_with_weights = FOREACH by_docs GENERATE
	group as uri,
	tfidf.(token,weight) as tokens; 

ordered = FOREACH docs_with_weights {
	sorted = ORDER tokens by weight desc;
	GENERATE 
	uri, sorted;	
};

--top = FOREACH ordered GENERATE
--	uri,
--	keepTopN(sorted) AS sorted;

--STORE top INTO '$OUTPUT_DIR/$LANG.tfidf_token_weights.json.bz2' USING JsonCompressedStorage();
STORE ordered INTO '$OUTPUT_DIR/$LANG.tfidf_token_weights.tsv' USING PigStorage('\t','-schema');

