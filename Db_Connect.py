import psycopg2
import pandas as pd

class DBUtil:

	def __init__(self, con, df_, dynamo_col,db_col_, schematable):
		self.con = con
		self.df_ = df_
		self.dynamo_col = dynamo_col
		self.db_col_ = db_col_
		self.schematable = schematable
		print("in init")
	

	def db_con(self):
		""" Connect to the Redshift database server """
		try:
			#read connection parameters
			configs = self.con
			print('Connecting to the Redshift database...')
			self.conn = psycopg2.connect(**configs)
			cur = self.conn.cursor()

			dataset = self.df_
			df = pd.DataFrame(dataset)
			pos = list(df.columns).index(self.dynamo_col)
			spc_col = [pos]
			cols = [0, 1, 2, 9, 10]
			did_df = df[df.columns[spc_col]]
			self.reg_df = df[df.columns[cols]]
			list_did_val = did_df.values.tolist()
			d_args_str = ','.join(cur.mogrify("(%s)", x).decode('utf-8') for x in list_did_val)
			#read_query = "Select * from {0} where did in (" + d_args_str + ")".format(wifi_edw_user.CXP_UPS_D_DEVICE_LIST_TMPC)
			read_query = "Select * from Redshift Table where did in (" + d_args_str + ")"
			org_row_count = len(self.reg_df.index)
			#print('\norg row count = ', org_row_count)
			return_pd = pd.read_sql(read_query, self.conn)
			#print(return_pd)
			self.conn.commit()
			cur.close()

		except (Exception, psycopg2.DatabaseError) as error:
			print(error)

		finally:
			self.conn.close()
			print('Database connection closed.')
			return(return_pd, org_row_count)



	def get_ins_rec(self,df_db):

		print("Creating the insert dataframe")
		df_file = self.reg_df
		#df_db = self.db_con()
		file_col = self.dynamo_col
		db_col = self.db_col_
		df_intersect_rec = None
		try:
			print("in Try block of get insert rec")
			#print("df cols : \n", df_file.columns)
			df_intersect_rec = df_file[~(df_file[file_col].isin(df_db[db_col]))].reset_index(drop=True)
		except ClientError as e:
			print(e)
		finally:
			print("Insert Dataframe completed")
			return(df_intersect_rec)


	def get_upd_rec(self,df_db):

		print("Creating the update dataframe")
		df_file = self.reg_df
		#df_db = self.db_con()
		file_col = self.dynamo_col
		db_col = self.db_col_

		df_same_rec = None
		try:
			print("in Try block of get update rec")
			#print("df cols : \n", df_file.columns)
			df_same_rec = df_file[(df_file[file_col].isin(df_db[db_col]))].reset_index(drop=True)
		except ClientError as e:
			print(e)
		finally:
			print("Update Dataframe completed")
			return (df_same_rec)