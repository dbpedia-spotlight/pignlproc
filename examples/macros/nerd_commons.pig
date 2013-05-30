
DEFINE read(WIKIPEDIA_DUMP, LANG, MIN_SURFACE_FORM_LENGTH) RETURNS ids, articles, pairs {
    -- parse Wikipedia into IDs, article texts and link pairs

    DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();
    DEFINE dbpediaEncode pignlproc.evaluation.DBpediaUriEncode('$LANG');

    -- Parse the wikipedia dump and extract text and links data
    parsed = LOAD '$WIKIPEDIA_DUMP'
      USING pignlproc.storage.ParsingWikipediaLoader('$LANG')
      AS (title, id, pageUrl, text, redirect, links, headers, paragraphs);

    -- Normalize pageUrls to DBpedia URIs
    parsed = FOREACH parsed GENERATE
      title,
      id,
      dbpediaEncode(pageUrl) AS pageUrl,
      text,
      dbpediaEncode(redirect) AS redirect,
      links,
      headers,
      paragraphs;

    -- Separate redirects from non-redirects
    SPLIT parsed INTO
      parsedRedirects IF redirect IS NOT NULL,
      parsedNonRedirects IF redirect IS NULL;

    -- Page IDs and titles
    $ids = FOREACH parsedNonRedirects GENERATE
      title,
      id,
      pageUrl;

    -- Articles
    $articles = FOREACH parsedNonRedirects GENERATE
      pageUrl,
      text,
      links,
      paragraphs;

    -- Build transitive closure of redirects
    redirects = redirectTransClo(parsedRedirects);
    -- Make redirects surface form occurrences
    pairsFromRedirects = FOREACH redirects GENERATE
      redirectSourceTitle AS surfaceForm,
      redirectTarget AS uri;

    -- Get Links
    pageLinksNonEmptySf = getLinks(articles, $LANG, $MIN_SURFACE_FORM_LENGTH);

    -- Resolve redirects
    pageLinksRedirectsJoin = JOIN
      redirects BY redirectSource RIGHT,
      pageLinksNonEmptySf BY uri;
    resolvedLinks = FOREACH pageLinksRedirectsJoin GENERATE
      surfaceForm,
      FLATTEN(resolve(uri, redirectTarget)) AS uri,
      pageUrl;
    distinctLinks = DISTINCT resolvedLinks;

    $pairs = UNION ONSCHEMA
      pairsFromRedirects,
      distinctLinks;
};

DEFINE getLinks(articles, LANG, MIN_SURFACE_FORM_LENGTH) RETURNS pageLinksNonEmptySf {
    -- get link pairs

    DEFINE dbpediaEncode pignlproc.evaluation.DBpediaUriEncode('$LANG');

    -- Extract sentence contexts of the links respecting the paragraph boundaries
    sentences = FOREACH $articles GENERATE
      pageUrl,
      FLATTEN(pignlproc.evaluation.SentencesWithLink(text, links, paragraphs))
      AS (sentenceIdx, sentence, targetUri, startPos, endPos);

    -- Project to three important relations
    pageLinks = FOREACH sentences GENERATE
      TRIM(SUBSTRING(sentence, startPos, endPos)) AS surfaceForm,
      dbpediaEncode(targetUri) AS uri,
      pageUrl AS pageUrl;

    -- Filter out surfaceForms that have zero or one character
    $pageLinksNonEmptySf = FILTER pageLinks
      BY SIZE(surfaceForm) >= $MIN_SURFACE_FORM_LENGTH;
};

DEFINE redirectTransClo(parsedRedirects) RETURNS redirects {
    -- Build transitive closure of redirects
    -- (resolve recursively) in 2 iterations
    r1 = red($parsedRedirects);
    r2 = red(r1);
    $redirects = FOREACH r2 GENERATE
      title AS redirectSourceTitle,
      pageUrl AS redirectSource,
      redirect AS redirectTarget;

    --FOR DEVELOPMENT ONLY: no transitive closure
    --redirects = FOREACH parsedRedirects GENERATE pageUrl AS redirectSource, redirect AS redirectTarget;
};

DEFINE red(parsedRedirects) RETURNS p {
    -- helper macro for redirectTransClo that is called "recursively"
    DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();

    r1a = FOREACH $parsedRedirects GENERATE
      title AS source1aTitle,
      pageUrl AS source1a,
      redirect AS target1a;
    r1b = FOREACH r1a GENERATE
      source1aTitle AS source1bTitle,
      source1a AS source1b,
      target1a AS target1b;
    j = JOIN
      r1a BY target1a LEFT,
      r1b BY source1b;
    $p = FOREACH j GENERATE
      source1aTitle AS title,
      source1a AS pageUrl,
      FLATTEN(resolve(target1a, target1b)) AS redirect;
};

DEFINE diskIntensiveNgrams(articles, MAX_NGRAM_LENGTH, LOCALE) RETURNS pageNgrams {
    -- create *all* ngrams in a bag

    DEFINE ngramGenerator pignlproc.helpers.RestrictedNGramGenerator('$MAX_NGRAM_LENGTH', '', '$LOCALE'); -- do not restrict: ''

    $pageNgrams = FOREACH $articles GENERATE
      FLATTEN(ngramGenerator(text)) AS ngram,
      pageUrl;
};

DEFINE memoryIntensiveNgrams(articles, pairs, MAX_NGRAM_LENGTH, TEMPORARY_SF_LOCATION, LOCALE) RETURNS pageNgrams {
    -- create ngrams that also are surface forms in the pairs bag

    allSurfaceForms = FOREACH pairs GENERATE
      surfaceForm;
    STORE allSurfaceForms INTO '$TEMPORARY_SF_LOCATION/surfaceForms';

    DEFINE ngramGenerator pignlproc.helpers.RestrictedNGramGenerator('$MAX_NGRAM_LENGTH', '$TEMPORARY_SF_LOCATION/surfaceForms', '$LOCALE');

    EXEC;

    -- filter to only include ngrams that are also surface forms while generating ngrams
    $pageNgrams = FOREACH $articles GENERATE
      FLATTEN( ngramGenerator(text) ) AS ngram,
      pageUrl PARALLEL 40;
};

DEFINE count(pairs, pageNgrams) RETURNS uriCounts, sfCounts, pairCounts, ngramCounts {
    -- count URIs, surface forms, pairs and ngrams

    -- Double links: if surface form is annotated once,
    -- it's annotated every time for one page.
    -- (Need left outer join because of tokenizer)
    doubledLinks = FOREACH ( JOIN
      $pairs BY (pageUrl, surfaceForm) LEFT,
      $pageNgrams BY (pageUrl, ngram) ) GENERATE
        surfaceForm,
        uri;

    -- Count pairs
    pairGrp = GROUP doubledLinks BY (surfaceForm, uri);
    $pairCounts = FOREACH pairGrp GENERATE
      FLATTEN($0) AS (pairSf, pairUri),
      COUNT($1) AS pairCount;

    -- Count surface forms
    sfGrp = GROUP doubledLinks BY surfaceForm;
    $sfCounts = FOREACH sfGrp GENERATE
      $0 AS surfaceForm,
      COUNT($1) AS sfCount;

    -- Count URIs
    uriGrp = GROUP doubledLinks BY uri;
    $uriCounts = FOREACH uriGrp GENERATE
      $0 AS uri,
      COUNT($1) AS uriCount;

    -- Count Ngrams
    ngrams = FOREACH $pageNgrams GENERATE
      ngram;
    ngramGrp = GROUP ngrams BY ngram;
    $ngramCounts = FOREACH ngramGrp GENERATE
      $0 as ngram,
      COUNT($1) AS ngramCount;
};

