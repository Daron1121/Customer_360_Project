import requests

response = requests.get('https://dummyjson.com/users?limit=150')

response = response.json()

#* Full Name extraction
# for user in response['users']:
#     print(f'{user['firstName']} {user['lastName']},')

for user in response['users']:
    print(f'{user['address']['coordinates']['lat']} : {user['address']['coordinates']['lng']},')