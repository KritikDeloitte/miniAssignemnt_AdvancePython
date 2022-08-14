from asyncio.windows_events import NULL
import csv
from decimal import Decimal
from distutils.log import error
from pickle import FALSE, NONE
from urllib import response
import uuid
from xml.dom.minidom import Attr
from boto3 import resource
import config
import helper_service

from boto3.dynamodb.conditions import Key, Attr
from werkzeug.security import generate_password_hash, check_password_hash

resource = resource(
    'dynamodb',
    endpoint_url = config.ENDPOINT_URL,
    aws_access_key_id     = config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY,
    region_name           = config.REGION_NAME
)

def create_table_movie():    
    table = resource.create_table(
        TableName = 'Movie', # Name of the table 
        KeySchema = [
            {
                'AttributeName': 'id',
                'KeyType'      : 'HASH' # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'id', # Name of the attribute
                'AttributeType': 'S'   # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits'  : 50,
            'WriteCapacityUnits': 50
        }
    )
    return table

def create_table_user():    
    table = resource.create_table(
        TableName = 'User', # Name of the table 
        KeySchema = [
            {
                'AttributeName': 'id',
                'KeyType'      : 'HASH' # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'id', # Name of the attribute
                'AttributeType': 'S'   # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits'  : 50,
            'WriteCapacityUnits': 50
        }
    )
    return table

user_table = resource.Table('User')

movie_table = resource.Table('Movie')

attributes = ['id', 'title', 'original_title', 'year', 'date_published', 'genre', 'duration', 'country', 'language', 'director', 'writer', 'production_company',
                      'actors', 'description', 'avg_vote', 'votes', 'budget', 'usa_gross_income', 'worlwide_gross_income', 'metascore', 'reviews_from_users', 'reviews_from_critics']

# To add all data of csv to DB
def write_to_movie():
    data = helper_service.convert_csv_to_list()
    for i in range(len(data)):
            dict_data={}
            j=0
            for key in attributes:
                dict_data[key]=data[i][j]
                j=j+1
            for key in attributes:
                if key not in dict_data:
                    dict_data[key]=""
            #print(dict_data)
            response = movie_table.put_item(
                
                  Item =  dict_data
                
            )
    
    return True

def write_to_user(username, password):
    hashed_password = generate_password_hash(password, method='sha256')
    id = str(uuid.uuid4())
    response = user_table.put_item(
        Item = {
            'id' : id,
            'username'  : username,
            'password'  : hashed_password,
        }
    )
    return response

def get_user(username):
    response = user_table.scan(
       FilterExpression= Attr('username').eq(username)
    )
    return response

def get_all_movie():
    return movie_table.scan().get('Items')


def find_movie_by_id(id):
    response = movie_table.get_item(
        Key = {
            'id'     : id
        },
        AttributesToGet = attributes
    )
    return response

def find_by_director_year(request_data):
   
    response = movie_table.scan(
        FilterExpression= Attr('director').eq(request_data['director']) & Attr('year').between(request_data['start_year'],request_data['end_year']),
        ProjectionExpression = 'title'
    )
    return response

def movie_review_filter(user_review):
    response= movie_table.scan(
        FilterExpression=Attr('reviews_from_users').gt(user_review) & Attr('language').eq('English'),
        ProjectionExpression = 'id,title,director,reviews_from_users,#c',
        ExpressionAttributeNames = {'#c': 'language'},
    )
    response['Items'] = sorted(response['Items'],key=lambda x:int(x['reviews_from_users']),reverse=True)
    return response

def budget_titles_filter(request_data):
    response =movie_table.scan(
    FilterExpression=Attr('country').eq(request_data['country']) & Attr('year').eq(request_data['year']),
    ExpressionAttributeNames = {'#c': 'year'},
    ProjectionExpression = 'id,title,budget,country,director,#c'
    )
    for i in response['Items']:
        helper_service.sort_budget_data(i)
    response['Items'] = sorted(response['Items'],key=lambda x: int(x['budget']) if len(x['budget'])>0 else 0 , reverse=True ) 
    return response


#Sync CSV file with DB

def sync_csv_with_db():
    with open('movies.csv', 'r' , errors='ignore') as csv_file:
        m =0 
        for row in reversed(list(csv.reader(csv_file))):

            val = find_movie_by_id(row[0])
            if 'Item' in val.keys():
                print("Noting to add in DB")
                break
            else:
                dict_data={}
                j=0
                for key in attributes:
                    dict_data[key]=row[j]
                    j=j+1
                response = movie_table.put_item(
                    Item =  dict_data
                    )
                print(response)
                print(dict_data)
                print('Row added')



            
