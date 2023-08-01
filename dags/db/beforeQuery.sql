select * from (select %(id)s, %(idSource)s, %(name)s, %(author)s, %(title)s, %(description)s, %(url)s, %(urlToImage)s, %(publishedAt)s, %(content)s) as tmp
where not exists (select id from njesusas_coderhouse.articles where id = %(id)s  and publishedAt = %(publishedAt)s)
LIMIT 1;