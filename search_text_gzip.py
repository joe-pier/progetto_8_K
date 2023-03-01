import pandas as pd
import re
import time
from concurrent.futures import ThreadPoolExecutor
import os
import gzip

######################################################


def get_text(txt_file_name):
    with gzip.open(txt_file_name, "rb") as file:
        data = file.read().decode('utf-8')
        return data

######################################################


def get_items(txt_file_name):
    items = list(set(re.findall("Item information:\t\t(\w.*)",
                 get_text(txt_file_name), re.IGNORECASE)))
    return ";".join(items)


def search_append_items(df_append_items):
    with ThreadPoolExecutor(max_workers=64) as executor:
        items = list(executor.map(get_items, list(df_append_items.path)))
    df_append_items["items"] = items
    return df_append_items

######################################################


def get_AGM_date(txt_file_name):
    agm_bool = bool(re.search("annual meeting of shareholders",
                    get_text(txt_file_name), re.IGNORECASE))
    return agm_bool


def search_append_date_AGMs(df_append_date_AGMs):
    with ThreadPoolExecutor(max_workers=64) as executor:
        AGM_date = list(executor.map(
            get_AGM_date, list(df_append_date_AGMs.path)))

    df_append_date_AGMs["AGM_date"] = list(AGM_date)
    return df_append_date_AGMs

######################################################

def create_dataset(year: str, sample_size: str):
    d = {}
    path = f"GZIP_y{year}_s{sample_size}/"
    dl = os.listdir(path)[1::] #exclude the compression report
    dl_path = [path+i for i in dl]
    d["path"]= dl_path

    df = pd.DataFrame(d)
    df.to_csv(f".gzip_paths/gzip_paths_y{year}_s{sample_size}.csv")


######################################################


def main():
    df = pd.read_csv(f".gzip_paths/gzip_paths_y{year}_s{sample_size}.csv")
    df.drop(list(df.filter(regex='Unnamed')), axis=1, inplace=True)

    # append items
    start_item_time = time.time()
    year_filtered_df_with_items = search_append_items(df).reset_index(drop=True)
    end_item_time = time.time()
    print("time for item extraction operation", end_item_time-start_item_time)

    # append bool AGMs:
    start_AGM_time = time.time()
    year_filtered_df_with_items_AGM_date = search_append_date_AGMs(year_filtered_df_with_items)
    end_AGM_time = time.time()
    print("time for AGM extraction operation", end_AGM_time-start_AGM_time)
    year_filtered_df_with_items_AGM_date.to_csv(f".OUTPUT/AGMs_excomp_y{year}_s{sample_size}.csv")
    



if __name__ == "__main__":
    year = 2016
    sample_size = 2000
    try:
        create_dataset(year = year, sample_size=sample_size)
    except:
        print(f"no folder named gzip_paths_y{year}_s{sample_size}.csv")
    finally:
        main()
