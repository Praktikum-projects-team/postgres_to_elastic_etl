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
    p.id as id,
    p.full_name,
    p.modified,
    COALESCE (
        json_agg(
            jsonb_build_object(
                'id', pfw_arr.film_work_id,
                'role', role
            )
        ) FILTER (WHERE pfw_arr.film_work_id is not null),
        '[]'
    ) as films
    FROM (
        select person_id, film_work_id, array_agg(role) as role
        FROM content.person_film_work
        GROUP BY person_id, film_work_id
        ORDER BY person_id
    ) as pfw_arr
    RIGHT JOIN content.person p on p.id = pfw_arr.person_id
    WHERE p.modified > %s
    GROUP BY p.id, full_name, modified
    ORDER BY p.modified;
'''


genre_statement = '''
    SELECT 
    g.id, 
    g.name,
    g.modified
    FROM content.genre g
    WHERE g.modified > %s
    ORDER BY g.modified;
'''