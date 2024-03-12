import flask
import os
import subprocess

app = flask.Flask(__name__)

""" Proper authentication and authorization should be implemented for these endpoints. """

STREAMINGESTAPPS_DIR = "code/stream_ingestion/streamingestapps"


@app.route('/<tenant_id>', methods=['GET'])
def list_ingestapps(tenant_id):
    """ Check which streamingestapps are available for a tenant """
    p = os.path.join(STREAMINGESTAPPS_DIR, tenant_id)
    apps = [f for f in os.listdir(p) if f.endswith('.py')]
    return flask.jsonify({'status': 'ok', 'message': apps})


@app.route('/<tenant_id>', methods=['PUT'])
def upload(tenant_id):
    """ Upload a new streamingestapp for a tenant """
    f = flask.request.files['file']

    if not f.filename.endswith('.py'):
        return flask.jsonify({'status': 'error', 'message': 'file must be a python file'})

    p = os.path.join(STREAMINGESTAPPS_DIR, tenant_id, f.filename)

    if not os.path.exists(os.path.dirname(p)):
        os.makedirs(os.path.dirname(p))

    f.save(p)

    return flask.jsonify({'status': 'ok', 'message': f'uploaded streamingestapp {f.filename}'})


@app.route('/start', methods=['POST'])
def start():
    """ Start a streamingestapp with the given arguments """
    tenant_id = flask.request.json['tenant_id']
    name = flask.request.json['name']
    args = flask.request.json['args']

    p = os.path.join(STREAMINGESTAPPS_DIR, tenant_id, name)
    if not os.path.exists(p):
        return flask.jsonify({'status': 'error', 'data': f'{name} does not exist'})

    proc = subprocess.Popen(['venv\\Scripts\\python.exe', p, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # check if the process started successfully
    try:
        out, err = proc.communicate(timeout=1)
        if proc.returncode != 0:
            return flask.jsonify(
                {'status': 'error', 'message': f'failed to start streamingestapp {name}', 'error': err.decode('utf-8')})
    except subprocess.TimeoutExpired:
        pass

    pid = proc.pid
    return flask.jsonify({'status': 'ok', 'message': f'started streamingestapp {name}', 'pid': pid})


@app.route('/stop', methods=['POST'])
def stop():
    """ Stop a streamingestapp """
    pid = flask.request.json['pid']

    try:
        os.kill(int(pid), 9)
    except ProcessLookupError:
        return flask.jsonify({'status': 'error', 'message': f'no process with pid {pid}'})
    except Exception as e:
        return flask.jsonify({'status': 'error', 'message': f'failed to stop process {pid}', 'error': str(e)})

    return flask.jsonify({'status': 'ok', 'message': f'stopped process {pid}'})


@app.route('/alerts', methods=['POST'])
def alert():
    """ Receive alerts from the monitoring system """
    data = flask.request.json
    print(
        f"Received alert for {data['origin']['tenant']}. Offending streaming app: {data['origin']['streaming_app_id']}")
    return flask.jsonify({'status': 'ok', 'message': f"received alert for {data['origin']['tenant']}"})


if __name__ == "__main__":
    app.run(host='localhost', port=5000)
