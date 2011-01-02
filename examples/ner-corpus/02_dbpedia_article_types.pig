/**
 * Join the wikipedia page URI to the DBpedia type of the linked entity.
 */

REGISTER $PIGNLPROC_JAR

-- Load wikipedia, instance types and redirects from dbpedia dumps
wikipedia_links = LOAD '$INPUT/wikipedia_links_$LANG.nt'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://xmlns.com/foaf/0.1/primaryTopic')
  AS (wikiuri: chararray, dburi: chararray);

wikipedia_links2 =
  FILTER wikipedia_links BY wikiuri IS NOT NULL;

-- Load dbpedia type data and filter out the overly generic owl:Thing type
instance_types =
  LOAD '$INPUT/instance_types_en.nt'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  AS (dburi: chararray, type: chararray);

instance_types_no_thing =
  FILTER instance_types BY type NEQ 'http://www.w3.org/2002/07/owl#Thing';

joined = JOIN instance_types_no_thing BY dburi, wikipedia_links2 BY dburi;

-- TODO: resolve the redirect info from dbpedia too

projected = FOREACH joined GENERATE wikiuri, type;

-- Ensure ordering for fast merge with sentence links
ordered = ORDER projected BY wikiuri ASC, type ASC;
STORE ordered INTO '$OUTPUT/$LANG/wikiuri_to_types';

