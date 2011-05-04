# Building a universal document classifier with Solr MoreLikeThis queries

TODO


    curl 'http://localhost:8983/solr/update/csv?commit=true&separator=%09&headers=false&fieldnames=id,popularity,text&stream.file=/path/to/topics_abstracts_1000.tsv&stream.contentType=text/plain;charset=utf-8'
