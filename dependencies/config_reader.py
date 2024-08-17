import json



def json_reader(file_name):
    with open(file_name, 'r') as f:
        data= json.load(f)
        f.close()
    return data
