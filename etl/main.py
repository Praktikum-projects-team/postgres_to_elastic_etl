from state import State, JsonFileStorage

storage = JsonFileStorage('state_file.json')
state = State(storage)
