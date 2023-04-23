select_fw_part = '''
    SELECT
    fw.id as id, 
    fw.title, 
    fw.description, 
    fw.rating as rating, 
    COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'role', pfw.role,
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id is not null),
       '[]'
    ) as persons,
    array_agg(DISTINCT g.name) as genres,
    fw.modified as modified
    FROM content.film_work fw
'''

fw_statement = select_fw_part + '''
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.modified > %s
    GROUP BY fw.id
    ORDER BY fw.modified;
'''

person_fw_statement = select_fw_part + '''
    JOIN content.person_film_work pfw1 ON pfw1.film_work_id = fw.id and pfw1.person_id = any(%s::uuid[])
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.modified <= %s
    GROUP BY fw.id
    ORDER BY fw.modified;
'''

genre_fw_statement = select_fw_part + '''
    JOIN content.genre_film_work gfw1 ON gfw1.film_work_id = fw.id and gfw1.genre_id = any(%s::uuid[])
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.modified <= %s
    GROUP BY fw.id
    ORDER BY fw.modified;
'''


person_statement = '''
    SELECT
    p.id,
    p.full_name,
    COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', pfw.film_work_id,
               'role', pfw.role
           )
       ) FILTER (WHERE pfw.film_work_id is not null),
       '[]'
    ) as films,
    p.modified
    FROM content.person p
    LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
    WHERE p.modified > %s
    GROUP BY p.id
    ORDER BY p.modified;
'''