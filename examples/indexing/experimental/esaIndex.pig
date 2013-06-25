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

-- TEST: set parallelism level for reducers
SET default_parallel 15;
    
SET job.name 'Wikipedia-Token-Counts-per-URI for $LANG';
--SET mapred.compress.map.output 'true';

--SET mapred.map.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define aliases
--DEFINE getTokens pignlproc.index.LuceneTokenizer('$STOPLIST_PATH', '$STOPLIST_NAME', '$LANG', '$ANALYZER_NAME');
--uncomment above and comment below to use default stoplist for the analyzer
--DEFINE getTokens pignlproc.index.LuceneTokenizer('$LANG', '$ANALYZER_NAME');
--DEFINE textWithLink pignlproc.evaluation.ParagraphsWithLink('$MAX_SPAN_LENGTH');
--DEFINE JsonCompressedStorage pignlproc.storage.JsonCompressedStorage();
DEFINE keepTopN pignlproc.helpers.FirstNtuples('$N');
DEFINE multiplyWeights pignlproc.index.esa.MultiplyWeights();
DEFINE divide pignlproc.index.esa.Divide();

-- Working notes:
-- we want the (tfidf value for the token in the resource) * (tfidf for the token in the II for res_j)
--Steps:
--(1) LOAD the inverted index and the tfidf index for the token in the resource  
--       - two loads --> inverted index and tfidf index (separate files)
-- UPDATE: this didn't work - requires far too much temporary storage (probably all of the flattens and joins


invertedIndex = LOAD '$INVERTED_INDEX'
  USING PigStorage('\t','-schema');

--Note: tests showed that the invertedIndex MUST be filtered in some way - look at the ESA paper for a more sophisticated method than 'keepTopN'
filteredIndex = FOREACH invertedIndex GENERATE
	token,
	keepTopN(sorted) AS sorted;
--DESCRIBE invertedIndex;
--raw_tsv: {token: chararray,sorted: {(uri: chararray,weight: double)}}

tfidfIndex = LOAD '$TFIDF_INDEX'
  USING PigStorage('\t','-schema'); 

uriTokenCount = FOREACH tfidfIndex GENERATE
	uri,
	COUNT(sorted) AS tokenCount: long;

--DUMP uriTokenCount;

--DESCRIBE uriTokenCount;

flattenTfidf = FOREACH tfidfIndex GENERATE
	uri AS uri, 
	FLATTEN(sorted) AS (token, weight);
--DUMP flattenTfidf;
--DESCRIBE tfidfIndex;
--DESCRIBE flattenTfidf;


--This creates NULLS if a token isn't known
uriByTokens = COGROUP flattenTfidf BY token INNER, filteredIndex BY token INNER;
DESCRIBE uriByTokens;
--working --> add field for the token's weihgt
flattenedTokens = FOREACH uriByTokens GENERATE 
	group AS token,
	FLATTEN(flattenTfidf.(uri,weight)) AS (uri,weight),
	FLATTEN(filteredIndex.sorted) AS resourceVector;
--DUMP flattenedTokens;
DESCRIBE flattenedTokens;

--TEST
filterNoNulls = FILTER flattenedTokens BY weight is not null;

--WORKING - now multiply the token's weight with each of the weights in the bag of resource tokens 
--	- pignlproc.index.esa.MultiplyWeights();
--SCHEMA: flattenedTokens: {token: chararray,uri: chararray,weight: double,resourceVector: {(token: chararray,weight: double)}}

--resTokenWeights = FOREACH filterNoNulls GENERATE
--	token AS token,
--	uri AS uri,
--      multiplyWeights(weight, resourceVector) AS sorted;
--DUMP resTokenWeights;
--STORE resTokenWeights INTO '$OUTPUT_DIR/token-vectors.tsv' USING PigStorage('\t', '-schema');
--DESCRIBE resTokenWeights;

resTokenWeights = FOREACH filterNoNulls GENERATE
	token,
	uri,
	FLATTEN(multiplyWeights(weight, resourceVector)) AS (resToken, resWeight);
--DUMP resTokenWeights;
--DESCRIBE resTokenWeights;
tokenGroups = GROUP resTokenWeights BY (uri, resToken);
--DUMP tokenGroups;
--DESCRIBE tokenGroups;
--STORE tokenGroups INTO '$OUTPUT_DIR/token-groups.tsv' USING PigStorage('\t', '-schema');

tokenGroupSum = FOREACH tokenGroups GENERATE
        FLATTEN(group) AS (uri, resToken), 	
	SUM(resTokenWeights.resWeight) AS totalWeight;

--DUMP tokenGroupSum;
--DESCRIBE tokenGroupSum;

allResTokens = GROUP tokenGroupSum BY uri;
--DUMP allResTokens;
DESCRIBE allResTokens;

resourceVecs = FOREACH allResTokens GENERATE
	group AS uri,
	tokenGroupSum.(resToken, totalWeight) as vector;
DESCRIBE resourceVecs;

withNumTokens = JOIN uriTokenCount BY uri, resourceVecs BY uri;
DESCRIBE withNumTokens;
--DUMP withNumTokens;

centroid = FOREACH withNumTokens {
	numTokens = uriTokenCount::tokenCount;
	ordered = ORDER resourceVecs::vector BY totalWeight DESC; 
	--UDF to divide by uriTokenCount
	GENERATE
	uriTokenCount::uri AS uri,
	divide(numTokens, ordered) AS resourceVec; 
};	
--DUMP centroid;
STORE centroid INTO '$OUTPUT_DIR/$LANG.esa-vectors.tsv' USING PigStorage('\t', '-schema');


--WORKING SECTION...
--STEPS:
--(1) Group by URI
--(2) Sum inside a hash (should this implement algebraic??)

--allResTokens = GROUP resTokenWeights BY uri;
--DUMP allResTokens;
--allResWeights = FOREACH allResTokens GENERATE
--	group AS uri,
--	resTokenWeights.sorted AS resVectors;

--centroid = FOREACH allResWeights GENERATE 
--	uri,
--      getCentroid(resVectors) AS resVector;	
	
--DUMP centroid;
--DESCRIBE allResWeights;	
--DESCRIBE centroid;


--uriTokenEsaWeight = FOREACH uriTokenEsaValue {
--	uriWeight = weight * invertedIndex::sorted::weight;
--	GENERATE
--	uri,
--	invertedIndex::sorted::uri AS uriForToken,
--	uriWeight AS tokensUriWeight;
--};
--
--allWeightsForUri = GROUP uriTokenEsaWeight BY (uri, uriForToken);
--	
--totalWeightForUri = FOREACH allWeightsForUri GENERATE 
--	group.uri AS uri,
--	group.uriForToken AS esaToken,
--	SUM(uriTokenEsaWeight.tokensUriWeight) AS summedWeight;
----DESCRIBE uriTokenEsaValue;
----DESCRIBE uriTokenEsaWeight;	
----DESCRIBE allWeightsForUri;
----DESCRIBE totalWeightForUri;
--
----now calculate the centroid value after joining with token count
--withTokenCount = JOIN totalWeightForUri BY uri, uriTokenCount BY uri;
--
----DESCRIBE withTokenCount;
--centroidValues = FOREACH withTokenCount {
--	totalWeight = totalWeightForUri::summedWeight;
--	numTokens = uriTokenCount::tokenCount;
--	avgWeight = totalWeight/numTokens;
--	GENERATE
--	totalWeightForUri::uri AS uri,
--	totalWeightForUri::esaToken AS esaToken,
--	avgWeight AS avgWeight;
--};
----DESCRIBE centroidValues;  
--
--allEsaTokens = GROUP centroidValues BY uri;
--
----DESCRIBE allEsaTokens;
--
--esaVectors = FOREACH allEsaTokens GENERATE
--	group AS uri,
--	centroidValues.(esaToken, avgWeight) AS esaVector;
--
----DESCRIBE esaVectors;
----DUMP esaVectors;
----STORE esaVectors INTO '$OUTPUT_DIR/$LANG.esa_vectors.tsv' USING PigStorage('\t','-schema');
--
--	
