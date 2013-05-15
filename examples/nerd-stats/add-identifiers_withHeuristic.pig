-- Load Custom-Wikipedia mapping
customWikiMapping = LOAD '$CUSTOM_WIKIPEDIA_MAPPING'
  AS (customUri:chararray, wikiId_custom:chararray);

-- Load Wikipedia-NERD-Stats
wikiNerdStats = LOAD '$WIKI_NERD_STATS'
  AS (title, surfaceForm, ofUri:double, uriOf:double, keyphraseness, wikiId_nerdStats, prominence);

-- Join
wikiNerdStatsJoin = JOIN
  customWikiMapping BY wikiId_custom RIGHT,
  wikiNerdStats BY wikiId_nerdStats;

-- Create Wikipedia-NERD-Stats with Custom-URIs
wikiNerdStatsWithCustomUris = FOREACH wikiNerdStatsJoin GENERATE
  title,
  surfaceForm,
  ofUri,
  uriOf,
  keyphraseness,
  customUri,
  wikiId_nerdStats,
  prominence;

---------------------------------------------------------------------

/*
Here we apply the follwing heurisitc:
The data set CUSTOM_SF_MAPPING contains ontology entities that do not have a Wikipedia ID
(in the required language), along side its surface forms.
These entities will be linked to Wikipedia entities if the Wikipedia surface form
that is longer than $MIN_SF_LEN_IF_NO_URI characters,
has an ofUri, that is larger than $MIN_OFURI_IF_NO_URI,
and an uriOf, that is larger than $MIN_URIOF_IF_NO_URI,
is not ambiguous in the dataset CUSTOM_SF_MAPPING.
*/

-- Load Custom-surface form mapping
customSfMappingRaw = LOAD '$CUSTOM_SF_MAPPING'
  AS (customUri:chararray, surfaceForm:chararray);
customSfMapping = DISTINCT customSfMappingRaw;
customSfMappingGroup = GROUP customSfMapping BY surfaceForm;
customSfMappingUnambiguous = FILTER customSfMappingGroup BY
  SIZE(customSfMapping) == 1;
customSfMappingUnambiguousFlat = FOREACH customSfMappingUnambiguous GENERATE
  FLATTEN(customSfMapping.customUri),
  group AS surfaceForm;

-- Split into data with and without URI
SPLIT wikiNerdStatsWithCustomUris INTO
  withUri IF (
       customUri IS NOT NULL
    OR SIZE(surfaceForm) < $MIN_SF_LEN_IF_NO_URI
    OR ofUri < $MIN_OFURI_IF_NO_URI
    OR uriOf < $MIN_URIOF_IF_NO_URI
  ),
  withoutUri IF (
        customUri IS NULL
    AND SIZE(surfaceForm) >= $MIN_SF_LEN_IF_NO_URI
    AND ofUri >= $MIN_OFURI_IF_NO_URI
    AND uriOf >= $MIN_URIOF_IF_NO_URI
  );

-- Join on the surface form in order to get a URI association
heuristicsJoin = JOIN
  withoutUri BY surfaceForm LEFT,
  customSfMappingUnambiguousFlat BY surfaceForm;
additionalUris = FOREACH heuristicsJoin GENERATE
  title,
  withoutUri::wikiNerdStats::surfaceForm,
  ofUri,
  uriOf,
  keyphraseness,
  customSfMappingUnambiguousFlat::customUri,
  wikiId_nerdStats,
  prominence;

-- Re-unite
result = UNION
  withUri,
  additionalUris;

-- Store heuristics result
STORE additionalUris INTO '$OUTPUT-urisOnlyByHeuristic';

-- Store
STORE result INTO '$OUTPUT';

