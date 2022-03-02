INSERT OR IGNORE
INTO files_tags (file_id, tag_id)
SELECT f.id, t.id
FROM (VALUES carray(?)) as l(tag_name)
JOIN files AS f ON t.path = ?
JOIN tags AS t ON t.name = l.tag_name
