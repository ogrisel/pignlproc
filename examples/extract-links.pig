-- Query incoming link popularity - local mode

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

-- Parse the wikipedia dump and extract text and links data
parsed =
  LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('en')
  AS (title, uri, text, links);

-- Flatten the links
links1 =
  FOREACH parsed
  GENERATE uri, flatten(links);

-- Select the target link
links2 =
  FOREACH links1
  GENERATE uri, value;

STORE links2 INTO '$OUTPUT' USING PigStorage();
