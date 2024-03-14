import flask
import os
import subprocess
import logging
import dotenv
import pathlib

dotenv.load_dotenv()

app = flask.Flask(__name__)

""" Proper authentication and authorization should be implemented for these endpoints. """

STREAMINGESTAPPS_DIR = "code/stream_ingestion/streamingestapps"

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    handlers=[logging.StreamHandler(), logging.FileHandler('logs/streamingestmanager.log')])

PYTHON = os.getenv('PYTHON_EXECUTABLE')

@app.route('/<tenant_id>', methods=['GET'])
def list_ingestapps(tenant_id):
    """ Check which streamingestapps are available for a tenant """
    logging.info(f"Listing streamingestapps for {tenant_id}")
    p = pathlib.Path(STREAMINGESTAPPS_DIR, tenant_id)
    apps = [f for f in os.listdir(p) if f.endswith('.py')]
    return flask.jsonify({'status': 'ok', 'message': apps})


@app.route('/start', methods=['POST'])
def start():
    """ Start a streamingestapp with the given arguments """
    tenant_id = flask.request.json['tenant_id']
    name = flask.request.json['name']
    args = flask.request.json['args']

    logging.info(f"Starting streamingestapp {name} for {tenant_id}")

    p = pathlib.Path(STREAMINGESTAPPS_DIR, tenant_id, name)
    if not p.exists():
        return flask.jsonify({'status': 'error', 'data': f'{name} does not exist'})

    p = p.with_suffix('').as_posix()
    p = p.replace('/', '.')
    proc = subprocess.Popen([PYTHON, "-m", p, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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

    logging.info(f"Stopping process {pid}")

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
    logging.warning(f"Received alert for {data['origin']['tenant']}")

    return flask.jsonify({'status': 'ok', 'message': f"received alert for {data['origin']['tenant']}"})


if __name__ == "__main__":
    app.run(host='localhost', port=5000)
