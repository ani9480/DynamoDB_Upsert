import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
import decimal
from pandas.io.json import json_normalize
import re
from botocore.exceptions import ClientError

class Read_Table:

	def __init__(self, session_type, pe_, ean_):
		self.session_type = session_type
		self.pe_ = pe_
		self.ean_ = ean_


	def choose_session(self):
		type_ = self.session_type
		if type_ == "session":
			session = boto3.Session(profile_name='name')
			dynamodb = session.resource('dynamodb')
		elif type_ == "client":
			dynamodb = boto3.resource('dynamodb')

		return dynamodb

	


	def read_dynamo(self):

		# Helper class to convert a DynamoDB item to JSON.
		class DecimalEncoder(json.JSONEncoder):
		    def default(self, o):
		        if isinstance(o, decimal.Decimal):
		            if o % 1 > 0:
		                return float(o)
		            else:
		                return int(o)
		        return super(DecimalEncoder, self).default(o)


		data = []
		dynamodb = self.choose_session()
		table = dynamodb.Table('Table Name')
		fe = Attr('UpdtTm').gt('2018-11-01');
		esk = None
		try:
			response = table.scan(FilterExpression=fe,
			#ProjectionExpression=self.pe_,#ExpressionAttributeNames= self.ean_
			)

			for i in response['Items']:
				data.append(i)

			while 'LastEvaluatedKey' in response:
				response = table.scan(
				#ProjectionExpression=self.pe_,
				FilterExpression=fe,
				#ExpressionAttributeNames= self.ean_,
				ExclusiveStartKey=response['LastEvaluatedKey'] )

				for i in response['Items']:
					data.append(i)
		except ClientError as e:
			if e.response['Error']['Code'] == "ConditionalCheckFailedException":
				print(e.response['Error']['Message'])
			else:
				raise
		else:
			f_input = json.dumps(data, cls=DecimalEncoder)
		return f_input


	def getdf(self):

		raw_data = json.loads(self.read_dynamo())
		data = json_normalize(raw_data)
		print(data.columns)





