import pandas as pd
from sqlalchemy import create_engine, text as sql_text

out_db_path = "/Users/alice/iCloud/Projects/hockey_db_data/20250530.db"
engine = create_engine(f"sqlite:///{out_db_path}")

asg_df = pd.read_sql(sql=sql_text("select * from ep_raw_asg"), con=engine.connect())

asg_df.to_csv("/Users/alice/iCloud/Projects/hockey_site/data/ep_raw_asg.csv", index=False)
