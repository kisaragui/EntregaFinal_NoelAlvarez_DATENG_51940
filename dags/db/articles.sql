CREATE TABLE IF NOT EXISTS njesusas_coderhouse.ARTICLES(
    id integer,
    idSource varchar(25),
    name varchar(25),
    author varchar(50),
    title varchar(500),
    description varchar(1500),
    url varchar(1500),
    urlToImage varchar(1500),
    publishedAt DATE,
    content varchar(1500)
) DISTKEY(publishedAt)
SORTKEY(publishedAt);