-- Query incoming link popularity - local mode

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

-- Parse the wikipedia dump and extract text and links data
parsed =
  LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('en')
  AS (title, uri, text, redirect, links);

filtered = FILTER parsed BY NOT redirect IS NULL;

-- Select the target link
redirects = FOREACH filtered GENERATE uri, redirect;

STORE redirects INTO '$OUTPUT' USING PigStorage();
