CREATE TABLE IF NOT EXISTS njesusas_coderhouse.ARTICLES(
    id integer,
    SourceId varchar(500),
    author varchar(500),
    title varchar(500),
    description varchar(1500),
    url varchar(1500),
    urlToImage varchar(1500),
    publishedAt DATE,
    content varchar(1500),
    SourceName varchar(25),
    language varchar(5),
    country varchar(5),
    category varchar(25)
) DISTKEY(publishedAt)
SORTKEY(publishedAt);