/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 */

SET job.name 'Wikipedia-NERD-Stats for $LANG'

%default DEFAULT_PARALLEL 20
SET default_parallel $DEFAULT_PARALLEL

-- enable compression of intermediate results
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;

REGISTER $PIGNLPROC_JAR
DEFINE defaultVal pignlproc.helpers.SecondIfNotNullElseFirst(); -- default values


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
-- normalize counts
--------------------

-- calculate p(uri|sf), a.k.a. "ofUri"
sfJoin = JOIN
  sfCounts BY surfaceForm,
  pairCounts BY pairSf;
probUriGivenSf = FOREACH sfJoin GENERATE
  surfaceForm,
  (double)pairCount/sfCount AS ofUri,
  pairUri;

-- calculate p(sf|uri), a.k.a. "uriOf"
uriJoin = JOIN
  uriCounts BY uri,
  pairCounts BY pairUri;
probSfGivenUri = FOREACH uriJoin GENERATE
  uri,
  (double)pairCount/uriCount AS uriOf,
  pairSf;

-- calculate p(sf), a.k.a "keyphraseness"
keyphraseJoin = JOIN
  sfCounts BY surfaceForm,
  ngramCounts BY ngram;
keyphraseness = FOREACH keyphraseJoin GENERATE
  surfaceForm,
  (double)sfCount/ngramCount AS keyphrasenessScore;

-- calculate p(uri)
-- not done at the moment


--------------------
-- output
--------------------

-- Join into one bag
joinAll1 = FOREACH ( JOIN
  probSfGivenUri BY (uri, pairSf),
  probUriGivenSf BY (pairUri, surfaceForm) ) GENERATE
    uri AS uri,
    surfaceForm AS surfaceForm,
    ofUri,
    uriOf;
joinAll2 = FOREACH ( JOIN
  joinAll1 BY surfaceForm LEFT, -- tokens(sf) > $MAX_NGRAM_LENGTH possible --> outer join
  keyphraseness BY surfaceForm ) GENERATE
    uri,
    joinAll1::surfaceForm,
    ofUri,
    uriOf,
    FLATTEN(defaultVal('0', keyphrasenessScore)) AS keyphraseness;
joinAll3 = FOREACH ( JOIN
  joinAll2 BY uri,
  uriCounts BY uri ) GENERATE
    uriCounts::uri AS uri,
    surfaceForm,
    ofUri,
    uriOf,
    keyphraseness,
    uriCount;
nerdStatsTable = FOREACH ( JOIN
  ids BY pageUrl,
  joinAll3 BY uri ) GENERATE
    title,
    surfaceForm,
    ofUri,
    uriOf,
    keyphraseness,
    id,
    uriCount;

-- Store
STORE nerdStatsTable INTO '$OUTPUT';

