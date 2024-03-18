from helperfunctions.dbsqlclient import ServerlessClient
from helperfunctions.transactions import Transaction, TransactionException, AlteredTableParser


class DBSQLTransactionManager(Transaction):

  def __init__(self, warehouse_id, mode="selected_tables", uc_default=False, host_name=None, token=None):

    super().__init__(mode=mode, uc_default=uc_default)
    self.host_name = host_name
    self.token = token
    self.warehouse_id = warehouse_id

    return 
  

  ### Execute multi statment SQL, now we can implement this easier for Serverless or not Serverless
  def execute_dbsql_transaction(self, sql_string, tables_to_manage=[], force=False, return_type="message"):

    ## return_type = message (returns status messages), last_result (returns the result of the last command in the sql chain)
    ## If force= True, then if transaction manager fails to find tables, then it runs the SQL anyways
    ## You do not NEED to run SQL this way to rollback a transaction,
    ## but it automatically breaks up multiple statements in one SQL file into a series of spark.sql() commands

    serverless_client = ServerlessClient(warehouse_id = self.warehouse_id, token=self.token, host_name=self.host_name) ## token=<optional>, host_name=<optional>verbose=True for print statements and other debugging messages
    
    result_df = None
    stmts = [i for i in sql_string.split(";") if len(i) >0]

    ## Save to class state
    self.raw_sql_statement = sql_string
    self.sql_statement_list = stmts

    success_tables = False

    try:
      self.begin_dynamic_transaction(tables_to_manage=tables_to_manage)

      success_tables = True

    except Exception as e:
      print(f"FAILED: failed to acquire tables with errors: {str(e)}")
    
    ## If succeeded or force = True, then run the SQL
    if success_tables or force:
      if success_tables == False and force == True:
        warnings.warn("WARNING: Failed to acquire tables but force flag = True, so SQL statement will run anyways")

      ## Run the Transaction Logic with Serverless Client
      try:
        print(f"TRANSACTION IN PROGRESS ...Running multi statement SQL transaction now\n")

        ###!! Since the DBSQL execution API does not understand multiple statements, we need to submit the USE commands in the correct order manually. This is done with the AlteredTableParser()

        ### Get the USE session tree and submit SQL statements according to that tree
        parser = AlteredTableParser()
        parser.parse_sql_chain_for_altered_tables(self.sql_statement_list)
        use_sessions = parser.get_use_session_tree()

        for i in use_sessions:

          session_catalog = i.get("session_cat")
          session_db = i.get("session_db")
          use_session_statemnts = i.get("sql_statements")

          for s in use_session_statemnts:
            single_st = s.get("statement")

            if single_st is not None:

              ## Submit the single command with the session USE scoped commands from the Parser Tree
              ## OPTION 1: return status message
              if return_type == "message":

                result_df = serverless_client.submit_multiple_sql_commands(sql_statements=single_st, use_catalog=session_catalog, use_schema=session_db)

              elif return_type == "last_result":
                
                result_df = serverless_client.submit_multiple_sql_commands_last_results(sql_statements=single_st, use_catalog=session_catalog, use_schema=session_db)

              else:
                result_df = None
                print("No run mode selected, select 'message' or 'last_results'")


        print(f"\n TRANSACTION SUCCEEDED: Multi Statement SQL Transaction Successfull! Updating Snapshot\n ")
        self.commit_transaction()


        ## Return results after committing sucesss outside of the for loop
        return result_df

          
      except Exception as e:
        print(f"\n TRANSACTION FAILED to run all statements... ROLLING BACK \n")
        self.rollback_transaction()
        print(f"Rollback successful!")
        
        raise(e)

    else:

      raise(TransactionException(message="Failed to acquire tables and force=False, not running process.", errors="Failed to acquire tables and force=False, not running process."))
      