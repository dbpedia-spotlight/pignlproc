/*
 * Wikipedia Statistics for Named Entity Recognition and Disambiguation
 *
 * @params $OUTPUT_DIR - the directory where the files should be stored
 *         $INPUT - the wikipedia XML dump
 *         $PIGNLPROC_JAR - the location of the pignlproc jar
 *         $LANG - the language of the Wikidump 
 *         $URI_LIST - a list of URIs to filter by
 *         $MAX_SPAN_LENGTH - the maximum length for a paragraph span
 */


-- TEST: set parallelism level for reducers
SET default_parallel 15;

SET job.name 'URI to context index for $LANG';

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define alias for tokenizer function
DEFINE concatenate pignlproc.helpers.Concatenate();
DEFINE textWithLink pignlproc.evaluation.ParagraphsWithLink('$MAX_SPAN_LENGTH');


--------------------
-- prepare
--------------------

-- Parse the wikipedia dump and extract text and links data
parsed = LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('$LANG')
  AS (title, id, pageUrl, text, redirect, links, headers, paragraphs);


uri_list = LOAD '$URI_LIST' 
   USING PigStorage()
   AS uri: chararray;

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

-- Extract paragraph contexts of the links 
paragraphs = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(textWithLink(text, links, paragraphs))
  AS (paragraphIdx, paragraph, targetUri, startPos, endPos);

filtered = JOIN paragraphs BY targetUri, uri_list BY uri USING 'replicated';

--Changes for indexing on small cluster
contexts = FOREACH filtered GENERATE
	paragraphs::targetUri AS uri,
	paragraphs::paragraph AS paragraph;

by_uri = GROUP contexts by uri;

filtered = FILTER by_uri by (COUNT(contexts.uri) > 20) AND (COUNT(contexts.uri)<100);

flattened = FOREACH filtered GENERATE
	group as uri,
	concatenate(contexts.paragraph) as context: chararray;

--it's not really important for the filtered ones to be ordered by uri
--ordered = order flattened by uri;

--Now output to .TSV --> Last directory in dir is hard-coded for now
STORE flattened INTO '$OUTPUT_DIR/uri_to_context_filtered.TSV.bz2' USING PigStorage('\t');

