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
    """ return list of items in the document """
    pattern_get_items = "Item information:\t\t(\w.*)"
    items = list(set(re.findall(pattern_get_items,
                 get_text(txt_file_name), re.IGNORECASE)))
    return ";".join(items)

def get_items_exec_comp(txt_file_name):
    """ return presence/absence of items related to executive compensation """
    print(get_items_exec_comp.count, end = "\r")
    pattern_get_items_exec_comp = "item 5.02"
    item_exec_comp = bool(re.findall(pattern_get_items_exec_comp,
                 get_text(txt_file_name), re.IGNORECASE))
    get_items_exec_comp.count += 1
    return item_exec_comp

def search_append_items(df_append_items):
    """ multithreading function (get_items, get_items_exec_comp) on documents """
    with ThreadPoolExecutor(max_workers=64) as executor:
        items = list(executor.map(get_items_exec_comp, list(df_append_items.path)))
    df_append_items["items"] = items
    return df_append_items

######################################################

def get_AGM_bool(txt_file_name):
    """ return presence or absence of information related to AGMs """
    print(get_AGM_bool.count, end = "\r")
    pattern_get_AGM_cool = "annual meeting of shareholders"
    agm_bool = bool(re.search(pattern_get_AGM_cool,
                    get_text(txt_file_name), re.IGNORECASE))
    return agm_bool

def search_append_AGMs(df_append_date_AGMs):
    """ multithreading function (get_AGM_bool) on documents"""
    with ThreadPoolExecutor(max_workers=64) as executor:
        AGM_date = list(executor.map(
            get_AGM_bool, list(df_append_date_AGMs.path)))
    df_append_date_AGMs["AGM"] = list(AGM_date)
    return df_append_date_AGMs

######################################################

def create_dataset(year: str, sample_size: str):
    """ create the gzip path csv containing the paths of .gz filings"""
    d = {}
    path = f"GZIP_y{year}_s{sample_size}/"
    dl = os.listdir(path)[1::] #exclude the compression report
    dl_path = [path+i for i in dl]
    d["path"] = dl_path
    df = pd.DataFrame(d)
    df.to_csv(f".gzip_paths/gzip_paths_y{year}_s{sample_size}.csv")


######################################################

def main():
    gzip_path_name = f"gzip_paths_y{year}_s{sample_size}.csv"
    df = pd.read_csv(os.path.join(".gzip_paths", gzip_path_name))
    df.drop(list(df.filter(regex='Unnamed')), axis=1, inplace=True)

    # append items
    print("item extraction started")
    start_item_time = time.time()
    year_filtered_df_with_items = search_append_items(df).reset_index(drop=True)
    end_item_time = time.time()
    item_total_time = end_item_time-start_item_time
    print("time for item extraction operation", item_total_time)

    # append bool AGMs:
    print("AGM extraction started")
    start_AGM_time = time.time()
    year_filtered_df_with_items_AGM_date = search_append_AGMs(year_filtered_df_with_items)
    end_AGM_time = time.time()
    agm_total_time = end_AGM_time-start_AGM_time
    print("time for AGM extraction operation", agm_total_time)
    year_filtered_df_with_items_AGM_date.to_csv(f".OUTPUT/AGMs_excomp_y{year}_s{sample_size}.csv")
    
    print("reading the compression report")
    with open(f"GZIP_y{year}_s{sample_size}/.report_y{year}_s{sample_size}.txt", "r") as compression_report:
        compression_report_data = compression_report.read()

    print("creating the extraction report")
    # create an extraction report for text extraction
    with open(f".extraction_reports/report_y{year}_s{sample_size}.txt", "w") as extraction_report:
        extraction_report.write(f"folder name: {gzip_path_name}\nitem extraction time: {item_total_time} s\nAGM extraction time: {agm_total_time} s\n\n{compression_report_data}")
    


if __name__ == "__main__":
    year = 2017
    sample_size = 70_000
    get_items_exec_comp.count = 0
    get_AGM_bool.count = 0
    try:
        if f"gzip_paths_y{year}_s{sample_size}.csv" in os.listdir(".gzip_paths"):
            pass
        else:
            create_dataset(year = year, sample_size=sample_size)
    except:
        print(f"no folder named gzip_paths_y{year}_s{sample_size}.csv")
    finally:
        try:
            main()
        except:
            print("retry")
