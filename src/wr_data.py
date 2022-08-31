import pandas as pd


def save_tocsv(df, filename, stage=None):
    
    if stage=="extract":
        if not df.empty:
            df.to_csv(f"log_data/{filename}_{stage}.csv", index=False)
            print(f"data hasil {stage} telah disimpan")
        else:
            print("Cek kembali isi dataframenya")
    
    if stage=="transform":
        if not df.empty:
            df.to_csv(f"log_data/{filename}_{stage}.csv", index=False)
            print(f"data hasil {stage} telah disimpan")
        else:
            print("Cek kembali isi dataframenya")
    
            
def read_data(filename):
    data = pd.read_csv(filename)
    return data

            
def clean_data(filename, list_task=None, list_column_to_clean=None, list_value_to_fill=None):
    
    df = read_data(filename)
    
    for i in range(len(list_task)):
    
        if list_task[i] == "parse_date":
            df[list_column_to_clean[i]] = pd.to_datetime(df[list_column_to_clean[i]])

        elif list_task[i] == "handle_nan":
            df[list_column_to_clean[i]].fillna(list_column_to_clean[i], inplace=True)
    return df

def insert_data(filename, con, table_name):
    df = read_data(f"log_data/{filename}")
    df.head(0).to_sql(name=table_name, con=con,if_exists="replace", index=False)
    df.to_sql(name=table_name, con=con,if_exists="append", index=False)
    print("data berhasil di insert")