
from CUST_TRVL_SRVC import *
from CXP_UPS import *
from Db_Connect import *
from update_insert import *
from config import config


class BeginProcess:

	def __init__(self, env, session_type):
		self.env = env
		self.session_type = session_type


	def read_param(self):
		with open("parameters.txt", "r") as f_input:
			
			json_content = json.load(f_input)
			env_detail = self.env
			for k,v in json_content[env_detail].items():
				if k == 'Table NAme':
					dict_ = v

			return(dict_)


	def connect_dynamo(self):
		
		envdict = self.read_param()
		pe = envdict["pe"]
		con = envdict["dbcon"]
		dynamo_col = envdict["dynamo_col"]
		db_col = envdict["db_col"]
		schematable = envdict["schematable"]
		#ean = envdict["ean"]
		#connect_CUST_TRVL = Read_CUST_TRVL(self.session_type,pe,ean)
		#connect_CUST_TRVL.getdf()
		connect_CXP_UPS = Read_CXP_UPS_T_DEVICE_LIST(self.session_type,pe)		
		cxp_data = connect_CXP_UPS.getdf()


		# print(con)
		# print(cxp_data.head())
		# print(dynamo_col)
		# print(db_col)
		# print(schematable)

		init_db_conn = DBUtil(con,cxp_data,dynamo_col,db_col,schematable)
		database_frame,org_row_count_ = init_db_conn.db_con()

		insert_data = init_db_conn.get_ins_rec(database_frame)
		update_data = init_db_conn.get_upd_rec(database_frame)
		

		update_insert_data = Upsert(con,insert_data,update_data,org_row_count_)
		update_insert_data.block_insert_update(pe,schematable,db_col)







def main():
    # Set name of class object
    start = BeginProcess("ENV TYPE","session")    
    start.connect_dynamo()

    #start.read_param()

if __name__ == "__main__":
    main()

