SELECT f.id
FROM files AS f
JOIN files_tags AS ft ON ft.file_id = f.id
JOIN tags AS t ON t.id = ft.tag_id
WHERE f.path = ? AND t.name IN carray(?)
GROUP BY f.id
HAVING COUNT(DISTINCT ft.tag_id) = ?
