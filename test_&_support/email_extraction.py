import requests

response = requests.get('https://dummyjson.com/users?limit=150')

response = response.json()
for user in response['users']:
    print(f'{user['email']},')