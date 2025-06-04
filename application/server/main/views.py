import redis
from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

from application.server.main.tasks import create_task_corrige, create_task_dwnload

from application.server.main.logger import get_logger

logger = get_logger(__name__)

# default_timeout = 4320000
default_timeout = 8640000

main_blueprint = Blueprint('main', __name__, )


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@main_blueprint.route('/corrige', methods=['POST'])
def run_task_harvest():
    """
    """
    args = request.get_json(force=True)
    print("Test Views", flush=True)
    conn = redis.from_url(current_app.config['REDIS_URL'])
    q = Queue(name='diplomes', connection=conn, default_timeout=default_timeout)
    task = q.enqueue(create_task_corrige, args)
    print("Test connexion", flush=True)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    print("Est-ce que ça marche ?", flush=True)
    return jsonify(response_object), 202


@main_blueprint.route('/dwnload', methods=['POST'])
def run_task_dwnload():
    """
    """
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        logger.debug("Connect Redis")
        q = Queue(name='patents', default_timeout=default_timeout, result_ttl=default_timeout)
        task = q.enqueue(create_task_dwnload())
    logger.debug("End task")
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

