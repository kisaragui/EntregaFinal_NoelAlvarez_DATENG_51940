select publishedAt, title, language from njesusas_coderhouse.articles 
where publishedAt = %(date_published)s;