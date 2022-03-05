SELECT f.id, f.path
FROM files AS f
JOIN tags AS t ON t.id = ft.tag_id
JOIN files_tags AS ft ON ft.file_id = f.id
WHERE t.name = ?
