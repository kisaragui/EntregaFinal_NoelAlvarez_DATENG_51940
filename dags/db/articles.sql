CREATE TABLE IF NOT EXISTS njesusas_coderhouse.ARTICLES(
    id integer,
    SourceId varchar(100),
    author varchar(100),
    title varchar(500),
    description varchar(1500),
    url varchar(1500),
    urlToImage varchar(1500),
    publishedAt DATE,
    content varchar(1500),
    SourceName varchar(25),
    country varchar(2),
    category varchar(25)
) DISTKEY(publishedAt)
SORTKEY(publishedAt);