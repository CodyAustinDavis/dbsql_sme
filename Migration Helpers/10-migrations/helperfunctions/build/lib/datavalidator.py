from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, concat, lit, sha2
from pyspark.sql import SparkSession

## This class extends row-counts into individual data values of two data frames.

## Data Validator hashes data frames / delta tables and compares row hash values against each other to more precisely compare the difference between 2 datasets. 

class DataValidator():

    def __init__(self):
        self.spark = SparkSession.getActiveSession()
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        self.spark.conf.set("spark.sql.shuffle.partitions", "auto")
        self.shuffle_partitions = shuffle_partitions if None else self.spark.sparkContext.defaultParallelism*2
        

    @staticmethod
    def replace_nulls_with_empty_str(df: DataFrame) -> DataFrame:
        for c in df.columns:
            df = df.withColumn(c, coalesce(col(c).cast("string"), lit('')))
        
        return df

    @staticmethod
    def concatenate_cols(df: DataFrame) -> DataFrame:
        return df.withColumn("concat_cols", concat(*df.columns).cast("string"))
    
    @staticmethod
    def concatenate_cols_subset(df: DataFrame, column_subset:[str]) -> DataFrame:
        return df.withColumn("concat_cols", concat(*column_subset).cast("string"))
    
    @staticmethod
    def get_sha_hash(df: DataFrame, col_name: str = 'concat_cols') -> DataFrame:
        return df.withColumn("sha_hash", sha2(col(col_name), 512))
    

    @staticmethod
    def get_sha_hash_column_subset(df: DataFrame, col_name: str = 'concat_cols') -> DataFrame:
        return df.withColumn("sha_hash", sha2(col(col_name), 512))
    

    ## Hashes an input dataframe and saves that hash table to memory temp table
    def create_hash_table_from_df(self, input_df: DataFrame):

        hash_table: DataFrame = (
            input_df
                .transform(self.replace_nulls_with_empty_str)
                .transform(self.concatenate_cols)
                .transform(self.get_sha_hash)
        )
        ## hash_table.createOrReplaceTempView(output_name)
        return hash_table
        

    ## Hash an existing delta tables and saves hash table to memory temp table
    def create_hash_table_from_input_table(self, tbl_name: str):

        hash_table: DataFrame = (
            self.spark.table(tbl_name)
                .transform(self.replace_nulls_with_empty_str)
                .transform(self.concatenate_cols)
                .transform(self.get_sha_hash)
        )

        return hash_table

    
    ## Hash an existing delta tables and saves hash table to memory temp table
    def create_hash_table_from_input_table_column_subset(self, tbl_name: str, column_subset:[str]):


        if len(column_subset) >= 1:

            hash_table: DataFrame = (
                self.spark.table(tbl_name)
                    .select(*column_subset)
                    .transform(self.replace_nulls_with_empty_str)
                    .transform(self.concatenate_cols)
                    .transform(self.get_sha_hash)
            )
        else: 
            hash_table: DataFrame = (
                self.spark.table(tbl_name)
                    .transform(self.replace_nulls_with_empty_str)
                    .transform(self.concatenate_cols)
                    .transform(self.get_sha_hash)
            )


            ## hash_table.createOrReplaceTempView(output_name)
        return hash_table


    ## Hash an existing delta tables and saves hash table to memory temp table
    def create_hash_table_from_input_df_column_subset(self, input_df: str, column_subset:[str]):

        if len(column_subset) >= 1:

            hash_table: DataFrame = (
                input_df
                    .select(*column_subset)
                    .transform(self.replace_nulls_with_empty_str)
                    .transform(self.concatenate_cols)
                    .transform(self.get_sha_hash)
            )
        else: 
            hash_table: DataFrame = (
                input_df
                    .transform(self.replace_nulls_with_empty_str)
                    .transform(self.concatenate_cols)
                    .transform(self.get_sha_hash)
            )


            ## hash_table.createOrReplaceTempView(output_name)
        return hash_table


    ## This function assumes the 2 tables you want to compare already exists but are NOT yet hashed
    def compare_two_delta_tables(self, original_table_name: str, comp_table_name: str) -> DataFrame:

        ## First create the hash tables for both
        clean_og_table_name = original_table_name.replace(".", "_")
        clean_comp_table_name = comp_table_name.replace(".", "_")

        og_df = self.create_hash_table_from_input_table(original_table_name)
        comp_df = self.create_hash_table_from_input_table(comp_table_name)

        og_df.createOrReplaceTempView("original_df")
        comp_df.createOrReplaceTempView("comparison_df")

        result_df : DataFrame = (self.spark.sql(f"""
                       
                        SELECT
                        COUNT( DISTINCT o.sha_hash) AS OriginalDistinctRecords,
                        COUNT( DISTINCT n.sha_hash) AS ComparisonDistinctRecords,
                        ((COUNT( n.sha_hash) - COUNT( o.sha_hash))::float / (COUNT( o.sha_hash)::float)) AS PercentDiffFromOriginal,
                        (COUNT( n.sha_hash) - COUNT(o.sha_hash))::int AS CountDiffFromOriginal,
                        COUNT(o.sha_hash) AS OriginalRecords,
                        COUNT(n.sha_hash) AS ComparisonRecords
                        FROM original_df o      
                        LEFT JOIN comparison_df n ON o.sha_hash = n.sha_hash
                       """)
        )

        return result_df


    def get_original_data_frame_without_matches(self, original_table_name: str, comp_table_name: str) -> DataFrame:

        ## First create the hash tables for both
        clean_og_table_name = original_table_name.replace(".", "_")
        clean_comp_table_name = comp_table_name.replace(".", "_")

        og_df = self.create_hash_table_from_input_table(original_table_name)
        comp_df = self.create_hash_table_from_input_table(comp_table_name)

        og_df.createOrReplaceTempView("original_df")
        comp_df.createOrReplaceTempView("comparison_df")

        result_df : DataFrame = (self.spark.sql(f"""
                       
                        SELECT
                        o.*
                        FROM original_df o      
                        LEFT ANTI JOIN comparison_df n ON o.sha_hash = n.sha_hash
                       """)
        )

        return result_df
    

    def get_original_data_frame_with_matches(self, original_table_name: str, comp_table_name: str) -> DataFrame:

        ## First create the hash tables for both
        clean_og_table_name = original_table_name.replace(".", "_")
        clean_comp_table_name = comp_table_name.replace(".", "_")

        og_df = self.create_hash_table_from_input_table(original_table_name)
        comp_df = self.create_hash_table_from_input_table(comp_table_name)

        og_df.createOrReplaceTempView("original_df")
        comp_df.createOrReplaceTempView("comparison_df")

        result_df : DataFrame = (self.spark.sql(f"""
                       
                        SELECT
                        o.*
                        FROM original_df o      
                        LEFT JOIN comparison_df n ON o.sha_hash = n.sha_hash
                        WHERE n.sha_hash IS NOT NULL
                       """)
        )

        return result_df
    

    ## This function assumes the 2 tables you want to compare already exists but are NOT yet hashed
    def compare_two_delta_tables_column_subset(self, original_table_name: str, comp_table_name: str, cols_to_compare: [str]) -> DataFrame:

        ## First create the hash tables for both
        clean_og_table_name = original_table_name.replace(".", "_")
        clean_comp_table_name = comp_table_name.replace(".", "_")

        og_df = self.create_hash_table_from_input_table_column_subset(original_table_name)
        comp_df = self.create_hash_table_from_input_table_column_subset(comp_table_name)

        og_df.createOrReplaceTempView("og_col_subset_tbl")
        comp_df.createOrReplaceTempView("comp_col_subset_tbl")

        result_df : DataFrame = (self.spark.sql(f"""
                       
                        SELECT
                        COUNT( o.sha_hash) AS OriginalHashedRecords,
                        COUNT( n.sha_hash) AS ComparisonHashedRecords,
                        ((COUNT( n.sha_hash) - COUNT( o.sha_hash))::float / (COUNT( o.sha_hash)::float)) AS PercentDiffFromOriginal,
                        (COUNT( n.sha_hash) - COUNT( o.sha_hash))::int AS CountDiffFromOriginal,
                        COUNT(o.sha_hash) AS OriginalRecords,
                        COUNT(n.sha_hash) AS ComparisonRecords
                        FROM og_col_subset_tbl o      
                        LEFT JOIN comp_col_subset_tbl n ON o.sha_hash = n.sha_hash
                       """)
        )

        return result_df
    

    ## This function does the hashing and saving for you if you just have 2 queries/dataframes you want to compare on the fly
    def compare_two_dfs(self, df_1: DataFrame, df_2: DataFrame) -> DataFrame:

        ## these are just in memory data frames, so they wont be named
        og_df = self.create_hash_table_from_df(df_1, "original_df")
        comp_df = self.create_hash_table_from_df(df_2, "comparison_df")

        og_df.createOrReplaceTempView("original_df")
        comp_df.createOrReplaceTempView("comparison_df")


        result_df : DataFrame = (self.spark.sql(f"""
                       
                        SELECT
                        COUNT( o.sha_hash) AS OriginalHashedRecords,
                        COUNT( n.sha_hash) AS ComparisonHashedRecords,
                        ((COUNT( n.sha_hash) - COUNT( o.sha_hash))::float / (COUNT( o.sha_hash)::float)) AS PercentDiffFromOriginal,
                        (COUNT( n.sha_hash) - COUNT(o.sha_hash))::int AS CountDiffFromOriginal,
                        COUNT(o.sha_hash) AS OriginalRecords,
                        COUNT(n.sha_hash) AS ComparisonRecords
                        FROM original_df o      
                        LEFT JOIN comparison_df n ON o.sha_hash = n.sha_hash
                       """)
        )

        return result_df   

    ## use this for comparing subset data frames (also used in building detailed column profile)
    ## both dfs need to contain the cols_to_compare list in common
    def compare_two_dfs_column_subset(self, df_1: DataFrame, df_2: DataFrame, cols_to_compare: [str]) -> DataFrame:

        ## these are just in memory data frames, so they wont be named
        og_df = self.create_hash_table_from_input_df_column_subset(df_1, cols_to_compare)#.repartition(self.shuffle_partitions)
        comp_df = self.create_hash_table_from_input_df_column_subset(df_2, cols_to_compare)#.repartition(self.shuffle_partitions)

        og_df.createOrReplaceTempView("temp_og_col_sub_df")
        comp_df.createOrReplaceTempView("temp_comp_col_sub_df")


        result_df : DataFrame = (self.spark.sql(f"""
                       
                        SELECT /*+ SKEW('n', 'sha_hash') */
                        COUNT( o.sha_hash) AS OriginalHashedRecords,
                        COUNT( n.sha_hash) AS ComparisonHashedRecords,
                        ((COUNT(  n.sha_hash) - COUNT(  o.sha_hash))::float / (COUNT(  o.sha_hash)::float)) AS PercentDiffFromOriginal,
                        (COUNT(  n.sha_hash) - COUNT(  o.sha_hash))::int AS CountDiffFromOriginal,
                        COUNT(  o.sha_hash) AS OriginalRecords,
                        COUNT(  n.sha_hash) AS ComparisonRecords
                        FROM temp_og_col_sub_df o      
                        LEFT JOIN (SELECT * FROM temp_comp_col_sub_df) n ON o.sha_hash = n.sha_hash
                       """)
                                 
        )

        return result_df


    ## These functions build a full column level profile that first builds the diff for the whole table then tries to explain the variance at a column level        

    def build_detailed_comp_profile_for_two_tables(self, df_1: DataFrame, df_2: DataFrame, cols_to_compare: [str] = None) -> DataFrame:

        ## these are just in memory data frames, so they wont be named
        og_df = self.create_hash_table_from_input_df_column_subset(df_1, cols_to_compare)#.repartition(self.shuffle_partitions)
        comp_df = self.create_hash_table_from_input_df_column_subset(df_2, cols_to_compare)#.repartition(self.shuffle_partitions)

        og_df.createOrReplaceTempView("temp_og_col_sub_df")
        comp_df.createOrReplaceTempView("temp_comp_col_sub_df")


        result_df : DataFrame = (self.spark.sql(f"""
                       
                        SELECT
                        COUNT( o.sha_hash) AS OriginalHashedRecords,
                        COUNT( n.sha_hash) AS ComparisonHashedRecords,
                        ((COUNT( n.sha_hash) - COUNT( o.sha_hash))::float / (COUNT( o.sha_hash)::float)) AS PercentDiffFromOriginal,
                        (COUNT( n.sha_hash) - COUNT(o.sha_hash))::int AS CountDiffFromOriginal,
                        COUNT(o.sha_hash) AS OriginalRecords,
                        COUNT(n.sha_hash) AS ComparisonRecords
                        FROM temp_og_col_sub_df o      
                        LEFT JOIN temp_comp_col_sub_df n ON o.sha_hash = n.sha_hash
                       """)
                                 
        )

        return result_df
      