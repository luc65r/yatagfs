INSERT OR IGNORE
INTO files_tags (file_id, tag_id)
SELECT f.id, t.id
FROM tags AS t
JOIN files AS f ON f.path = ?
WHERE t.name IN carray(?)
