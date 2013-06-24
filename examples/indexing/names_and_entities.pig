/*
 * DBpedia Spotlight Statistics
 */

-- IMPORTANT: Run this script with "pig -no_multiquery", otherwise the surface forms
-- that are passed to the distributed cache are not available in $TEMPORARY_SF_LOCATION
-- before they are required by pignlproc.helpers.RestrictedNGramGenerator.


SET job.name 'DBpedia Spotlight: Names and entities for $LANG'
SET mapred.child.java.opts '-Xmx2048m'

%default DEFAULT PARELLEL 20
SET default_parallel $DEFAULT_PARALLEL

-- enable compression of intermediate results
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;

SET io.sort.mb 1024

REGISTER $PIGNLPROC_JAR
DEFINE dbpediaEncode pignlproc.evaluation.DBpediaUriEncode('$LANG'); -- URI encoding
DEFINE default pignlproc.helpers.SecondIfNotNullElseFirst(); -- default values


--------------------
-- read and count
--------------------
IMPORT '$MACROS_DIR/nerd_commons.pig';

-- Get surfaceForm-URI pairs
ids, articles, pairs = read('$INPUT', '$LANG', $MIN_SURFACE_FORM_LENGTH);

-- Make ngrams
pageNgrams = diskIntensiveNgrams(articles, $MAX_NGRAM_LENGTH);
--pageNgrams = memoryIntensiveNgrams(articles, pairs, $MAX_NGRAM_LENGTH, $TEMPORARY_SF_LOCATION, $LOCALE);

-- Count
uriCounts, sfCounts, pairCounts, ngramCounts = count(pairs, pageNgrams);


--------------------
-- join some results
--------------------

-- Join annotated and unannotated SF counts:
sfAndTotalCounts = FOREACH (JOIN
  sfCounts    BY surfaceForm LEFT OUTER,
  ngramCounts BY ngram) GENERATE surfaceForm, sfCount, ngramCount;


--------------------
-- Output
--------------------

STORE pairCounts INTO '$OUTPUT/pairCounts';
STORE uriCounts INTO '$OUTPUT/uriCounts';
STORE sfAndTotalCounts INTO '$OUTPUT/sfAndTotalCounts';
