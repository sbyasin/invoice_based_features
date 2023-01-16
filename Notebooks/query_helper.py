from typing import Tuple

from google.cloud import bigquery

DEFAULT_PROJECT = 'fb-data-science-dev'


class SafeQuery:
  def __init__(
    self,
    query:str,
    max_dollar=1,
    max_gb:float=None,
    project:str=None,
  ):  
    self.query = query
    self.max_dollar = max_dollar
    self.max_gb = max_gb
    self.project = project
      
    if not project:
      print("Default project is missing, query might fail.")
      
  def estimate_cost(self, print_out=True) -> Tuple[float,float]:
    # Construct a BigQuery client object.
    if self.project:
      client = bigquery.Client(project=self.project)
    else:
      client = bigquery.Client()
    
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    # Start the query, passing in the extra configuration.
    query_job = client.query(
      query=self.query,
      job_config=job_config,
    )  # Make an API request.

    # A dry run query completes immediately.
    #print("This query will process {} bytes.".format(query_job.total_bytes_processed))
    
    query_cost = (query_job.total_bytes_processed / 1024 ** 4) * 5
    mb_processed = query_job.total_bytes_processed/10**6
    
    if print_out:
      print(f"Query will process {round(mb_processed/10**3,1)} GB and it will cost {round(query_cost,2)} dollars.")
    
    return (mb_processed, query_cost)
  
  def get_data_as_df(self) -> Tuple[float,float]:
    # Construct a BigQuery client object.
    if self.project:
      client = bigquery.Client(project=self.project)
    else:
      client = bigquery.Client()
    
    # Start the query, passing in the extra configuration.
    query_job = client.query(
      query=self.query,
    )  # Make an API request.
    
    data_df = query_job.to_dataframe(progress_bar_type='tqdm')    

    return data_df