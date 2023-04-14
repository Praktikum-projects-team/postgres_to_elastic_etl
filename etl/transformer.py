# "id": str,
# "imdb_rating": float,
# "genre": list[str],
# "title": str,
# "description": list[str],
# "director": list[str],
# "actors_names": list[str],
# "writers_names": list[str],
# "actors": {
#         "id": str,
#         "name": str
# },
# "writers": {
#         "id": str,
#         "name": str
# }


def transform(batch: dict):
    transformed_data = []
    for filmwork in batch:
        filmwork_transformed = {
            'id': filmwork['id'],
            'imdb_rating': filmwork['rating'],
            'genre': filmwork['genres'],
            'title': filmwork['title'],
            'description': filmwork['description'],
            'director': [person['name'] for person in filmwork['persons'] if person['role'] == 'director'],
            'actors_names': [person['name'] for person in filmwork['persons'] if person['role'] == 'actor'],
            'writers_names': [person['name'] for person in filmwork['persons'] if person['role'] == 'writer'],
            'actors': [
                {
                    'id': person['id'], 'name': person['name']
                } for person in filmwork['persons'] if person['role'] == 'actor'
            ],
            'writers': [
                {
                    'id': person['id'], 'name': person['name']
                } for person in filmwork['persons'] if person['role'] == 'writer'
            ],
        }
        transformed_data.append(filmwork_transformed)
    return transformed_data