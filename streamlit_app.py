from unittest import result
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.types import Variant
import snowflake.snowpark.functions as F

#from config import snowflake_conn_prop
session = Session.builder.configs(snowflake_conn_prop).create()

oag_schedule_df = session.table("OAG_SCHEDULE").limit(10)
oag_schedule_df = oag_schedule_df.select("*", sql_expr("DEPCTRY || '_' || DEP_PORT_CD_ICAO AS rep_airp"))

aerospace_data_atlas_df = session.table('"AEROSPACE_DATA_ATLAS"."AEROSPACE"."avia_paoa-20200501"').select("*").limit(10)
st.write(aerospace_data_atlas_df.to_pandas())
aerospace_data_atlas_filtered_df = aerospace_data_atlas_df \
  .filter(aerospace_data_atlas_df.col('"tra_cov"') == lit("TOTAL"))\
  .filter(aerospace_data_atlas_df.col('"schedule"') == lit("TOT"))\
  .filter(aerospace_data_atlas_df.col('"tra_meas"') == lit("PAS_BRD"))\
  .filter(aerospace_data_atlas_df.col('"Date"') == lit("2021-09-01"))


st.write(oag_schedule_df.to_pandas())
result = oag_schedule_df.join(aerospace_data_atlas_filtered_df, "left", oag_schedule_df.col("rep_airp") == aerospace_data_atlas_df.col('"rep_airp"'))
st.write(result.to_pandas())
