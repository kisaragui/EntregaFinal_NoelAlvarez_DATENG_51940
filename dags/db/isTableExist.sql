SELECT table_name 
FROM information_schema.tables 
WHERE table_name='articles' 
AND table_catalog = 'data-engineer-database'
AND table_schema = 'njesusas_coderhouse';