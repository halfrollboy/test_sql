Переменные для базы данных находятся в .env файле
WITH RECURSIVE r AS (
   SELECT id, parentid, name, type, 1 AS level
   FROM office
   WHERE id = 3

   UNION ALL

   SELECT office.id, office.parentid, office.name, office.type, r.level + 1 AS level
   FROM office
      JOIN r
          ON office.id = r.parentid
)

WITH RECURSIVE main AS (
   SELECT id, parentid, name, 1 AS level
   FROM office
   WHERE id = (select r.id from r where r.type = 1)

   UNION ALL

   SELECT office.id, office.parentid, office.name, main.level + 1 AS level
   FROM office
      JOIN main
          ON office.parentid = main.id
)

SELECT * FROM main where ;