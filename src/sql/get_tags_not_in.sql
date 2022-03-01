SELECT id, name
FROM tags
WHERE name NOT IN carray(?)
