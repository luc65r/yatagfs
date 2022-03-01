SELECT f.id, f.path
FROM files AS f
JOIN files_tags AS ft ON ft.file_id = f.id
WHERE ft.tag_id IN carray(?)
GROUP BY f.id
HAVING COUNT(DISTINCT ft.tag_id) = ?
