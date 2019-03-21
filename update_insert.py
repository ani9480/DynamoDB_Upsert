import psycopg2
import pandas as pd
import time

class Upsert:

	def __init__(self, con, insert_df, update_df,org_row_count_):
		self.con = con
		self.insert_df = insert_df
		self.update_df = update_df
		self.org_row_count_ = org_row_count_



	def create_string(self, pe,schematable, db_col):
		col_list = pe.split(",")
		col_list_new = [i for i in col_list if i != db_col]
		length = len(col_list_new) - 1
		new_string = """UPDATE {0} SET""".format(schematable)
		counter = 0
		for s in col_list_new:
			if col_list_new.index(s) != length:
				new_string = new_string + """ {0} = '{1}',""".format(s, {counter})
				counter+=1
			else:
				new_string = new_string + """ {0} = '{1}' WHERE """.format(s, {counter})
				counter+=1

		new_string = new_string + """ {0} = '{1}';""".format(db_col, {counter})
		return(new_string)




	def update_string(self,cur,db_col,pe):
		upd_rows_df = self.update_df
		upd_row_count = len(upd_rows_df.index)
		print('\norg row count = ', self.org_row_count_)
		print('upd row count = ', upd_row_count)
		# print('\norg row count = ', org_row_count)
		#print(upd_rows_df.columns)
		col_list = pe.split(",")
		col_list_drop = ['col', 'col', 'col', 'col']
		col_list_new = [i for i in col_list if i in col_list_drop]		
		col_list_new.append(db_col)
		
		upd_rows_df = upd_rows_df.reindex(col_list_new, axis=1)
		#print(upd_rows_df.columns)
		list_val_update = upd_rows_df.values.tolist()
		args_str_update = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", x).decode('utf-8') for x in list_val_update)
		#print(args_str_update)


		if upd_row_count != 0:

			def update_(row):#Use the foor loop on pe
				sql_string_update = """UPDATE Redshift TAble SET col = '{0}', col = '{1}', col = '{2}',col = '{3}' WHERE col = '{4}';"""

				sql_ = sql_string_update.format(row['col'],row['col'],row['col'],row['col'],row['col'])
				return sql_

			update_list = list(upd_rows_df.apply(update_, axis=1).values)
			update_string_ = ''.join(map(str, update_list))
			return(update_string_)
		else:
			return(upd_row_count)
		


	def block_insert_update(self, pe, schematable, db_col):

		configs = self.con
		conn = psycopg2.connect(**configs)
		cur = conn.cursor()
		df = self.insert_df
		update_string_ = self.create_string(pe,schematable, db_col)
		print(update_string_)
		up_string = self.update_string(cur,db_col,pe)
		ins_row_count = len(df.index)
		print('ins row count = ', ins_row_count)		
		print('Upserting to the Redshift database...')		
		list_val = df.values.tolist()
		if len(list_val) != 0 and up_string != 0:

			#print("val:", list_val)
			args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", x).decode('utf-8') for x in list_val)
			#print(args_str)
			start_time = time.time()
			ins_query = "INSERT INTO Redshift Table Values "
			try:
				cur.execute(ins_query + args_str)
				cur.execute(up_string)
				# cur.execute(read_query)
				print("---It took %s seconds to upsert---" % (time.time() - start_time))
			except (Exception, psycopg2.DatabaseError) as error:
				print(error)
			finally:
				conn.commit()
				cur.close()

		elif up_string != 0 and len(list_val) == 0:
			start_time = time.time()
			try:
				cur.execute(up_string)
				# cur.execute(read_query)
				print("---It took %s seconds to upsert---" % (time.time() - start_time))
			except (Exception, psycopg2.DatabaseError) as error:
				print(error)
			finally:
				conn.commit()
				cur.close()
				
		elif up_string == 0 and len(list_val) != 0:
			args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", x).decode('utf-8') for x in list_val)
			#print(args_str)
			start_time = time.time()
			ins_query = "INSERT INTO Redshift Table Values "
			try:
				cur.execute(ins_query + args_str)
				# cur.execute(read_query)
				print("---It took %s seconds to upsert---" % (time.time() - start_time))
			except (Exception, psycopg2.DatabaseError) as error:
				print(error)
			finally:
				conn.commit()
				cur.close()


	




	

	




