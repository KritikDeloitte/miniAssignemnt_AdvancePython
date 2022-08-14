
from asyncio.windows_events import NULL
import datetime
from functools import wraps
from pickle import NONE
import time
from urllib import response
from xml.dom.minidom import TypeInfo
from flask import Flask, request,g , jsonify ,make_response
import jwt
import model as dynamodb
from werkzeug.security import generate_password_hash, check_password_hash
import config
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)


sched = BackgroundScheduler(daemon=True)
sched.add_job(dynamodb.sync_csv_with_db,'interval',seconds=10)
sched.start()


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']

        if not token:
            return jsonify({'message' : 'Token is missing!'}), 401

        try: 
            data = jwt.decode(token, config.SECRET_KEY)
        except:
            return jsonify({'message' : 'Token is invalid!'}), 401

        return f( *args, **kwargs)

    return decorated


@app.route('/createTable')
@token_required
def root_route():
    dynamodb.create_table_movie()
    return 'Table created'

@app.route('/create_user_table')
def create_user_table():
    dynamodb.create_table_user()
    return {
        'msg' : 'User table created'
    }

@app.route('/login')
def login():
    data = request.get_json()
    username = data['username']
    password = data['password']
    response = None
    if not username or not password:
        response = make_response('Could not verify, Incorrect username or password', 401, {'WWW-Authenticate' : 'Basic realm="Login required!"'})

    user_response = dynamodb.get_user(username)
    user = NONE
    if (user_response['ResponseMetadata']['HTTPStatusCode'] == 200 and len(user_response['Items']) >0):
        user = user_response['Items'][0]
    else:
        return {'msg':'User does not exist'}
    if check_password_hash(user['password'], password):
        token = jwt.encode({'id' : user['id'], 'exp' : datetime.datetime.utcnow() + datetime.timedelta(minutes=120)}, config.SECRET_KEY)

        response = jsonify({'token' : token.decode('UTF-8')})
    else:
        response = {'msg':'Password Incorrect'}
    return response

@app.route('/add_user',methods=['POST'])
def add_user():
    user_data = request.get_json()
    response= dynamodb.write_to_user(user_data['username'],user_data['password'])
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return { 'msg' : 'User Added' }
    return {
        'msg': 'Some error occured',
        'response': response
    }


@app.route('/csv_to_db', methods=['POST'])
@token_required
def add_data_from_csv():
    response = dynamodb.write_to_movie()
    if (response):
        return {
            'msg': 'Data Added successfully',
        }
    return {  
        'msg': 'Some error occcured'
    }

@app.route('/get_all_movie')
@token_required
def find_all_movie():
    return {"movieList": dynamodb.get_all_movie()}

@app.route('/filter_by_director',methods=['GET'])
@token_required
def filter_by_director():
    data = request.get_json()
    response = dynamodb.find_by_director_year(data)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return { 'response': response['Items'] }
        return { 'msg' : 'Item not found!' }
    return {
        'msg': 'Some error occured',
        'response': response
    }

@app.route('/movie_with_user_review',methods=['GET'])
@token_required
def filter_by_user_review():
    user_review = int(request.args.get('user_review'))
    
    response = dynamodb.movie_review_filter(user_review)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return { 'response': response['Items'] }
        return { 'msg' : 'Item not found!' }
    return {
        'msg': 'Some error occured',
        'response': response
    }

@app.route('/highest_budget_titles',methods=['GET'])
@token_required
def filter_highest_budget_titles():
    data = request.get_json()
    response = dynamodb.budget_titles_filter(data)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return { 'response': response['Items'] }
        return { 'msg' : 'Item not found!' }
    return {
        'msg': 'Some error occured',
        'response': response
    }   

# This runs before every request
@app.before_request
def before_request():
  g.start = time.time()


# This runs after every request
@app.after_request
def after_request(response):
    diff = int((time.time() - g.start) * 1000)
    response.headers["X-TIME-TO-EXECUTE"] = "{} ms".format(diff)
    return response

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001, debug=True)