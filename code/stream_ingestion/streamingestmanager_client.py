import sys

import requests


class StreamingestManagerClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def stop(self, pid):
        url = f"http://{self.host}:{self.port}/stop"
        response = requests.post(url, json={'pid': pid})
        return response.json()

    def start(self, tenant_id, name, args):
        url = f"http://{self.host}:{self.port}/start"
        data = {'tenant_id': tenant_id, 'name': name, 'args': args}
        response = requests.post(url, json=data)
        return response.json()

    def list_ingestapps(self, tenant_id):
        url = f"http://{self.host}:{self.port}/{tenant_id}"
        response = requests.get(url)
        return response.json()


if __name__ == "__main__":
    client = StreamingestManagerClient('localhost', 5000)
    if sys.argv[1] == 'list':
        print(client.list_ingestapps('tenant1'))
    if sys.argv[1] == 'start':
        response = client.start('tenant1', 'streamingestor.py', ["tenant1", "tenant1_measurements"])
        print(response)
    if sys.argv[1] == 'stop':
        pid = sys.argv[2]
        print(client.stop(pid))
