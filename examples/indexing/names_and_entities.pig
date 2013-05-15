/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 */

-- IMPORTANT: Run this script with "pig -no_multiquery", otherwise the surface forms
-- that are passed to the distributed cache are not available in $TEMPORARY_SF_LOCATION
-- before they are required by pignlproc.helpers.RestrictedNGramGenerator.

-- TODO use macros (Pig > 0.9) to make the script more readable (esp. redirect resolution)


SET job.name 'DBpedia Spotlight: Names and entities for $LANG'
SET mapred.child.java.opts '-Xmx2048m'

-- enable compression of intermediate results --TODO how much does performance suffer?
--set io.sort.mb 1024

--SET pig.tmpfilecompression true;
--SET pig.tmpfilecompression.codec gz;

--SET default_parallel 20;

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

-- Define alias for redirect resolver function
DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();

--------------------
-- prepare
--------------------

-- Parse the wikipedia dump and extract text and links data
parsed = LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('$LANG')
  AS (title, id, pageUrl, text, redirect, links, headers, paragraphs);

-- filter as early as possible
SPLIT parsed INTO
  parsedRedirects IF redirect IS NOT NULL,
  parsedNonRedirects IF redirect IS NULL;

-- Wikipedia IDs
ids = FOREACH parsedNonRedirects GENERATE
  title,
  id,
  pageUrl;

-- Load Redirects and build transitive closure
-- (resolve recursively) in 2 iterations
r1a = FOREACH parsedRedirects GENERATE
  title AS source1aTitle,
  pageUrl AS source1a,
  redirect AS target1a;
r1b = FOREACH r1a GENERATE
  source1aTitle AS source1bTitle,
  source1a AS source1b,
  target1a AS target1b;
r1join = JOIN
  r1a BY target1a LEFT,
  r1b BY source1b;

r2a = FOREACH r1join GENERATE
  source1aTitle AS source2aTitle,
  source1a AS source2a,
  flatten(resolve(target1a, target1b)) AS target2a;
r2b = FOREACH r2a GENERATE
  source2aTitle AS source2bTitle,
  source2a AS source2b,
  target2a AS target2b;
r2join = JOIN
  r2a BY target2a LEFT,
  r2b BY source2b;

redirects = FOREACH r2join GENERATE
  source2aTitle AS redirectSourceTitle,
  source2a AS redirectSource,
  FLATTEN(resolve(target2a, target2b)) AS redirectTarget;

--FOR DEVELOPMENT ONLY: no transitive closure
--redirects = FOREACH parsedRedirects GENERATE pageUrl AS redirectSource, redirect AS redirectTarget;

-- Project articles
articles = FOREACH parsedNonRedirects GENERATE
  pageUrl,
  text,
  links,
  paragraphs;

-- Extract sentence contexts of the links respecting the paragraph boundaries
sentences = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(pignlproc.evaluation.SentencesWithLink(text, links, paragraphs))
  AS (sentenceIdx, sentence, targetUri, startPos, endPos);

-- Project to three important relations
pageLinks = FOREACH sentences GENERATE
  TRIM(SUBSTRING(sentence, startPos, endPos)) AS surfaceForm,
  targetUri AS uri,
  pageUrl AS pageUrl;

-- Filter out surfaceForms that have zero or one character
pageLinksNonEmptySf = FILTER pageLinks
  BY SIZE(surfaceForm) >= $MIN_SURFACE_FORM_LENGTH;

-- Resolve redirects
pageLinksRedirectsJoin = JOIN
  redirects BY redirectSource RIGHT,
  pageLinksNonEmptySf BY uri;
resolvedLinks = FOREACH pageLinksRedirectsJoin GENERATE
  surfaceForm,
  FLATTEN(resolve(uri, redirectTarget)) AS uri,
  pageUrl;

distinctLinks = DISTINCT resolvedLinks;
DESCRIBE distinctLinks;

--WORKING 16.4.13
pairsFromRedirects = FOREACH redirects GENERATE
  redirectSourceTitle AS surfaceForm,
  redirectTarget AS uri;

sfs = UNION ONSCHEMA
   (FOREACH distinctLinks GENERATE surfaceForm as sf),
   (FOREACH pairsFromRedirects GENERATE surfaceForm as sf);

STORE sfs INTO '$TEMPORARY_SF_LOCATION/sfs';

-- Define Ngram generator with maximum Ngram length
DEFINE ngramGenerator pignlproc.helpers.RestrictedNGramGenerator('$MAX_NGRAM_LENGTH', '$TEMPORARY_SF_LOCATION/sfs', '$LOCALE');

EXEC;

-- Make Ngrams (filter to only include ngrams that are also surfaceforms)
pageNgrams = FOREACH articles GENERATE FLATTEN( ngramGenerator(text) ) AS ngram, pageUrl PARALLEL 40;

-- Double links: if surface form is annotated once,
-- it's annotated every time for one page.
-- (Need left outer join because of tokenizer)
doubledLinks = JOIN
  distinctLinks BY (pageUrl, surfaceForm) LEFT,
  pageNgrams BY (pageUrl, ngram);

pairsFromLinks = FOREACH doubledLinks GENERATE
  surfaceForm,
  uri;

pairs = UNION ONSCHEMA
  pairsFromRedirects,
  pairsFromLinks;

--------------------
-- count
--------------------

-- Count pairs: absolute
pairGrp = GROUP pairs BY (surfaceForm, uri);
pairCounts = FOREACH pairGrp GENERATE
  FLATTEN($0) AS (pairSf, pairUri),
  COUNT($1) AS pairCount;

STORE pairCounts INTO         '$OUTPUT/pairCounts';

-- Count pairs: per page
distinctPairGrp = GROUP distinctLinks BY (surfaceForm, uri);
pagePairCounts = FOREACH distinctPairGrp GENERATE
  FLATTEN($0) AS (pagePairSf, pagePairUri),
  COUNT($1) AS pagePairCount;

-- Count surface forms
sfGrp = GROUP pairs BY surfaceForm;
sfCounts = FOREACH sfGrp GENERATE
  $0 AS surfaceForm,
  COUNT($1) AS sfCount;

-- Count URIs
uriGrp = GROUP pairs BY uri;
uriCounts = FOREACH uriGrp GENERATE
  $0 AS uri,
  COUNT($1) AS uriCount;

STORE uriCounts INTO          '$OUTPUT/uriCounts';

-- Count Ngrams: absolute
ngrams = FOREACH pageNgrams GENERATE
  ngram;
ngramGrp = GROUP ngrams BY ngram;
ngramCounts = FOREACH ngramGrp GENERATE
  $0 as ngram,
  COUNT($1) AS ngramCount;

--------------------
-- calculate results
--------------------

-- Join annotated and unannotated SF counts:
sfAndTotalCounts = FOREACH (JOIN
  sfCounts    BY surfaceForm LEFT OUTER,
  ngramCounts BY ngram) GENERATE surfaceForm, sfCount, ngramCount;

--------------------
-- Output
--------------------

-- Store
STORE sfAndTotalCounts INTO   '$OUTPUT/sfAndTotalCounts';
