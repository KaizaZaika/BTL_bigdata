
import requests
import time
import json
import requests
import socket
import os

LANGUAGES = ['Python', 'Java', 'JavaScript']

HOST = "0.0.0.0"   
PORT = 9999
TOKEN = "token" + os.getenv('TOKEN')
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    with conn:
        print("Connected... Starting sending data.")
        while True:
            for langauge in LANGUAGES:
                url = 'https://api.github.com/search/repositories?q=language:{}&sort=updated&order=desc&per_page=50'.format(langauge)
                try:
                    response = requests.get(url, headers={"Authorization": TOKEN})
                    if response.ok:
                        data = response.json()
                        repositories = data["items"]
                        for repository in repositories:
                            if repository["language"] is None or repository["language"] not in LANGUAGES:
                                continue
                            data = {
                                'id': repository["id"],
                                'name': repository["name"],
                                'language': repository["language"],
                                'description': repository["description"],
                                'stargazers_count': repository["stargazers_count"],
                                'pushed_at': repository["pushed_at"]
                            }
                            json_data = f"{json.dumps(data)}\n".encode()
                            conn.send(json_data)
                        print("Sent {} repositories for langauge: {}".format(len(repositories), langauge))
                    else:
                        print("Error:", response.status_code, response.reason)
                except KeyboardInterrupt:
                    s.shutdown(socket.SHUT_RD)

            time.sleep(15)   