from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from pyspark.sql import DataFrame
import hashlib
import uuid

### This is the new temp DB process on Unity Catalog. It does NOT use both table paths and table names, as they 
### Catalogs and databases for temp db are sharable, but delta helpers will track the session Id for each table it creates, and will ONLY create/delete tables that contain that session Id to avoid overwrites / conflicts


#### TO DO: 
"""
1. Think about how to robustly drop/recreate tables from zombie sessions (maybe the session is no longer alive but it didnt get to drop its tables). Currently we fail the process, but we might want to add an option of allowing the new session to override it. -- I actually think I need to persisnt the table session data to a delta table to manage active sessions and clean up zombie sessions.

2. Add ability to drop/re-create/override tables in different scopes. i.e. its ok to re-create/drop tables/schemas if they were created by the same user/codepath and only the session is different. 
"""

class DeltaHelpers():

    
    def __init__(self, catalog="delta_helpers", db_name="temp_db", **kwargs):
        
        self.spark = SparkSession.getActiveSession()
        self.temp_db = db_name
        self.catalog = catalog
        self.catalog_temp = self.catalog + '_temp'
        self.dbutils = None
        self.session_id = str(uuid.uuid4())
        ### Save list of tables that are being managed by this instance
        self.active_temp_tables = {self.session_id: []}
      
        #if self.spark.conf.get("spark.databricks.service.client.enabled") == "true":
        try:     
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(self.spark)
        
        except:
            
            import IPython
            self.dbutils = IPython.get_ipython().user_ns["dbutils"]

        self.source_code_path =  self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        self.active_user = None

        #### The function depends on DBR on whether we use current_user() or session_user()
        try: 
            ## DBR 14.1+
            self.active_user = self.spark.sql("SELECT session_user()").collect()[0][0]
        except:
            ## DBR < 14.1
            self.active_user = self.spark.sql("SELECT current_user()").collect()[0][0]


        self.spark.sql(f"""CREATE CATALOG IF NOT EXISTS {self.catalog_temp}""")

        if self.spark.catalog.databaseExists(f"{self.catalog_temp}.{self.temp_db}"):
            ## Check if it was created by same session, if so that is ok, if not, warn the user but it is ok to share database level, they just cant step on each other tables
                schema_session_id = self.spark.sql(f"""SELECT tag_value FROM system.information_schema.schema_tags
                                                  WHERE catalog_name = '{self.catalog_temp}'
                                                  AND schema_name = '{self.temp_db}'
                                                  AND tag_name = 'session_id'
                                                  """).collect()[0][0]
                
                schema_session_user = self.spark.sql(f"""SELECT tag_value FROM system.information_schema.schema_tags
                                                  WHERE catalog_name = '{self.catalog_temp}'
                                                  AND schema_name = '{self.temp_db}'
                                                  AND tag_name = 'session_user'
                                                  """).collect()[0][0]
                
                schema_session_code_path = self.spark.sql(f"""SELECT tag_value FROM system.information_schema.schema_tags
                                                  WHERE catalog_name = '{self.catalog_temp}'
                                                  AND schema_name = '{self.temp_db}'
                                                  AND tag_name = 'source_code_path'
                                                  """).collect()[0][0]
                
                if schema_session_id == self.session_id:
                    ### Database is already created by this session, nothing to do!
                    pass

                elif schema_session_id != self.session_id:
                    print(f"""INFO: Existing schema session Id: {schema_session_id}, user: {schema_session_user}, code_path: {schema_session_code_path} \n vs Current active session id: {self.session_id}, user: {self.active_user}, code_path: {self.source_code_path}
                          """)

                    if schema_session_code_path == self.source_code_path:
                        if schema_session_user == self.active_user:
                            print(f"""INFO: The database you are using for temp is is aleady created by another session, but the source code path and user are the same, so this is likely safe if you are just starting over a code run or running a new run. If you have CONCURRENT runs of the same code/user, then seriously consider carefully using the same temp_db instance for sharing temp tables - be sure to create separate table names per concurrent run. """)
                        elif schema_session_user != self.active_user:
                            print(f"""INFO: The database you are using for temp is is aleady created by another session and different user, but the source code path is the same, so this is likely safe if you are just starting over a code run or running a new run. If you have CONCURRENT runs of the same code/user, then seriously consider carefully using the same temp_db instance for sharing temp tables - be sure to create separate table names per concurrent run. """)

                    elif schema_session_code_path != self.source_code_path: 
                            print(f"""WARNING: The database you are using for temp db is already created by another session and different code path and different user. This is ok, as there are still table-level protections, but it is better to use separate temp dbs for separate processes. Schema tags will always only be created by the original creator of the schema.
                            """)

                    else:
                        print(f"""WARNING: The database you are using for temp db is already created by another session. This is ok, as there are still table-level protections, but it is better to use separate temp dbs for separate processes. Schema tags will always only be created by the original creator of the schema.
                            """)

        ## IF database does NOT exist
        else:           
            self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.catalog_temp}.{self.temp_db}""")

            #### Tag creator entity of database
            self.spark.sql(f"""ALTER DATABASE {self.catalog_temp}.{self.temp_db} SET TAGS ('session_id' = '{self.session_id}', 'source_code_path'='{self.source_code_path}', 'session_user'='{self.active_user}')""")

        self.session_info = {'session_id': self.session_id, 'source_code_path': self.source_code_path, 'session_user': self.active_user, 'temp_catalog': self.catalog_temp, 'temp_db': self.temp_db}

        self.spark.sql(f"""USE CATALOG {self.catalog_temp}""")
        self.spark.sql(f"""USE DATABASE {self.temp_db}""")

        print(f"Initializing temp_db for session id {self.session_id} under user {self.active_user} at {self.catalog_temp}.{self.temp_db}")

        return
    
    
    def getSessionId(self):
        return self.session_id
    
    def getSessionUser(self):
        return self.active_user
    
    def getSessionSourceCodePath(self):
        return self.source_code_path

    def getAllSessionInformation(self):
        return self.session_info
    

    def getSessionTableDf(self, table_name):
        ### Get the table as long as it is in the session
        session_table_info = None

        session_table_info = [i for i in self.active_temp_tables.get(self.session_id) if i.get('table_name') == table_name][0]

        session_temp_df = self.spark.table(session_table_info.get('full_table_name'))
        return session_temp_df
    
    def getSessionTableInfo(self, table_name):
        ### Get the table as long as it is in the session
        session_table_info = None

        session_table_info = [i for i in self.active_temp_tables.get(self.session_id) if i.get('table_name') == table_name][0]

        return session_table_info
    
    
    def getAllSessionTableInformation(self):
        return self.active_temp_tables.get(self.session_id)
    

    @staticmethod
    def hash_string(input_string: str) -> str:
        # Create a new sha256 hash object
        sha256_hash = hashlib.sha256()
        
        # Update the hash object with the bytes of the input string
        sha256_hash.update(input_string.encode('utf-8'))
        
        # Get the hexadecimal representation of the hash
        hashed_string = sha256_hash.hexdigest()
        return hashed_string
    

    ### Create or replace delta temp table
    def saveToDeltaTempTable(self, df: DataFrame, table_name: str, mode="overwrite", auto_return_results_df= False):
        
        ## modes = "overwrite", "append"
        if mode not in ["append", "overwrite"]:
            raise(ValueError("ERROR: Mode not in append or overwrite"))
        

        full_table_name = f"{self.catalog_temp}.{self.temp_db}.{table_name}"

        ## Create or replace table depending on mode
        try: 

            #### Apply tags / comments of:
            # session_id
            # code path
            # current_user()

            ### Only append/overwrite if it does not exists or if it is tagged with the current session Id
            if self.spark.catalog.tableExists(full_table_name):

                ## IF succeeds, then the table exists, now check if it is under the same session id as the active session
                table_session_id = self.spark.sql(f"""SELECT tag_value FROM system.information_schema.table_tags
                                                  WHERE catalog_name = '{self.catalog_temp}'
                                                  AND schema_name = '{self.temp_db}'
                                                  AND table_name = '{table_name}'
                                                  AND tag_name = 'session_id'
                                                  """).collect()[0][0]
                
                table_session_user = self.spark.sql(f"""SELECT tag_value FROM system.information_schema.table_tags
                                                  WHERE catalog_name = '{self.catalog_temp}'
                                                  AND schema_name = '{self.temp_db}'
                                                  AND table_name = '{table_name}'
                                                  AND tag_name = 'session_user'
                                                  """).collect()[0][0]
                
                table_session_code_path = self.spark.sql(f"""SELECT tag_value FROM system.information_schema.table_tags
                                                  WHERE catalog_name = '{self.catalog_temp}'
                                                  AND schema_name = '{self.temp_db}'
                                                  AND table_name = '{table_name}'
                                                  AND tag_name = 'source_code_path'
                                                  """).collect()[0][0]
                
                if table_session_id is None:
                    ## table does not exists, can create / replace
                    df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

                    ## Tag this table with the session info to ensure other entities do not delete it
                    self.spark.sql(f"""ALTER TABLE {full_table_name} SET TAGS ('session_id' = '{self.session_id}', 'source_code_path'='{self.source_code_path}', 'session_user'='{self.session_user}')""")

                    ## Save full name of table
                    full_table_dict = {"catalog": self.catalog_temp, "database": self.temp_db, "table_name": table_name, "full_table_name": full_table_name, 'session_id': self.session_id, 'code_path': self.source_code_path, 'session_user': self.active_user}
                    
                    self.active_temp_tables[self.session_id].append(full_table_dict)


                elif table_session_id != self.session_id: 

                    ### TO DO: optionally, we can add a flag to throw and error or just overwrite the table with a different session_id. I think it SHOULD error by default, because that will notify the user to use a different temp_db name/scope
                    raise(RuntimeError(f"Table is created and managed by another session! Existing table session Id: {table_session_id}, user: {table_session_user}, code_path: {table_session_code_path} \n vs Current active session id: {self.session_id}, user: {self.active_user}, code_path: {self.source_code_path}"))
                
                elif table_session_id == self.session_id:

                    ### If table exists and its the same session, you can respect the write mode
                    df.write.format("delta").mode(mode).saveAsTable(full_table_name)

                    ## Tag this table with the session info to ensure other entities do not delete it
                    self.spark.sql(f"""ALTER TABLE {full_table_name} SET TAGS ('session_id' = '{self.session_id}', 'source_code_path'='{self.source_code_path}', 'session_user'='{self.session_user}')""")

                    ## Save full name of table

                    ### Check and make sure the table metadata already exists

                    if len([i for i in self.active_temp_tables.get(table_session_id) if i.get('full_table_name') == full_table_name]) > 0:
                        pass

                    else:
                        print(f"Could not find metadata for table {full_table_name} in session metadata. Adding now. ")
                        full_table_dict = {"catalog": self.catalog_temp, "database": self.temp_db, "table_name": table_name, "full_table_name": full_table_name, 'session_id': self.session_id, 'code_path': self.source_code_path, 'session_user': self.active_user}
                        
                        self.active_temp_tables[self.session_id].append(full_table_dict)


            ## If table does not exist, create it, always overwrite mode just in case
            ## Also, if table does not exists, check the existance of the schema first
            else:
                ## If Schema does not exist (like we hard-deleted it, then create and tag correctly)
                if not self.spark.catalog.databaseExists(f"{self.catalog_temp}.{self.temp_db}"):

                    print(f"Schema does not yet exist, creating: {self.catalog_temp}.{self.temp_db}")
                    self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.catalog_temp}.{self.temp_db}""")

                    #### Tag creator entity of database
                    self.spark.sql(f"""ALTER DATABASE {self.catalog_temp}.{self.temp_db} SET TAGS ('session_id' = '{self.session_id}', 'source_code_path'='{self.source_code_path}', 'session_user'='{self.active_user}')""")

                    

                df.write.format("delta").mode(mode).saveAsTable(full_table_name)

                ## Tag this table with the session info to ensure other entities do not delete it
                self.spark.sql(f"""ALTER TABLE {full_table_name} SET TAGS ('session_id' = '{self.session_id}', 'source_code_path'='{self.source_code_path}', 'session_user'='{self.active_user}')""")

                ## Save full name of table
                full_table_dict = {"catalog": self.catalog_temp, "database": self.temp_db, "table_name": table_name, "full_table_name": full_table_name, "session_id": self.session_id, "code_path": str(self.source_code_path), "session_user": str(self.active_user)}
                
                self.active_temp_tables[self.session_id].append(full_table_dict)
                

        except Exception as e:
            raise(e)
        
        ## optionally return results in same function (to use like spark.sql())
        if auto_return_results_df:
            ## Only return the table if it is in the session tables
            full_table_name = str([i.get('full_table_name') for i in self.active_temp_tables.get(self.session_id) if i.get('session_id') == self.session_id][0])
            return self.spark.table(full_table_name)
        
        return
    
    ### Remove a temp table for session
    def removeDeltaTempTable(self, table_name):

        #### check if table is in the registry - only delete tables in the registry, its possible there are other tables with the same name that you do not want to delete

        full_table_name = [i.get("full_table_name") for i in self.active_temp_tables.get(self.session_id) if i.get("table_name") == table_name and i.get('session_id') == self.session_id][0]

        print(f"Deleting {full_table_name} in session: {self.session_id}")
        
        try: 
            self.spark.sql(f"""DROP TABLE IF EXISTS full_table_name""")
            ## Remove the deleted table
            self.active_temp_tables = {self.session_id: [i for i in self.active_temp_tables.get(self.session_id) if i.get("table_name") != table_name and i.get('session_id') == self.session_id]}

            print(f"Temp Table: {full_table_name} has been deleted in session: {self.session_id}")

        except Exception as e:
            raise(e)

        return
    
    def removeAllTempTablesForSession(self):

        full_tables_to_remove = [i.get('full_table_name') for i in self.active_temp_tables.get(self.session_id) if i.get("session_id") == self.session_id]
        print(f"Removing all tables for session: {self.session_id} in {self.catalog_temp}.{self.temp_db}. \n Tables to be removed: {', '.join(full_tables_to_remove)}")

        for tbl in full_tables_to_remove:
            self.spark.sql(f"DROP TABLE IF EXISTS {tbl}")
            
        print(f"All temp tables in the session have been removed: {self.catalog_temp}.{self.temp_db} in session: {self.session_id}")
        return
    
    ## This method drops the temp db if it exists for your catalog/db combo. If force_drop = True, then it will drop the database no matter who created it. If False, then it will ONLY drop the database if it was created by the active session Id. 
    def dropTempDb(self, force=False):

        schema_session_id = self.spark.sql(f"""
                            SELECT tag_value FROM system.information_schema.schema_tags 
                            WHERE catalog_name = '{self.catalog_temp}' AND schema_name = '{self.temp_db}' AND tag_name = 'session_id'""").collect()[0][0]
        
        ## If database was created by same session, its ok to drop
        if schema_session_id is None:
            print(f"Cannot find temp DB to drop, skipping step.")

        elif schema_session_id == self.session_id:
                        self.spark.sql(f"DROP DATABASE IF EXISTS {self.catalog_temp}.{self.temp_db} CASCADE")

        elif schema_session_id != self.session_id:
            if force:
                self.spark.sql(f"DROP DATABASE IF EXISTS {self.catalog_temp}.{self.temp_db} CASCADE")
            else:
                raise(RuntimeError("ERROR: Trying to delete a temp db that was created by another session!"))
            
        else:
            pass
    
        return
        