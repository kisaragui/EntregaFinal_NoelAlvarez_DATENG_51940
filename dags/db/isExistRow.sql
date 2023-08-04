select * from (select %(id)s, %(SourceId)s, %(author)s, %(title)s, %(description)s, %(url)s, %(urlToImage)s, %(publishedAt)s, %(content)s, %(sourceName)s, %(country)s, %(category)s ) as tmp
where not exists (select id from njesusas_coderhouse.articles where id = %(id)s  and publishedAt = %(publishedAt)s)
LIMIT 1;