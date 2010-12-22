-- Query incoming link popularity - local mode

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

parsed =
  LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('en')
  AS (title, uri, text, redirect, links, headers, paragraphs);

shingles =
  FOREACH parsed
  GENERATE uri, pignlproc.analysis.Shingle(3,text );


--tmp = LIMIT joined2 10; DUMP tmp;
STORE shingles INTO '$OUTPUT' USING PigStorage();
