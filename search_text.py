import pandas as pd
import re
import time
from concurrent.futures import ThreadPoolExecutor
import gzip


######################################################


def get_text(txt_file_name):
    with open(txt_file_name) as file:
        data = file.read()
        return data


def get_gzip(txt_file_name):
    with gzip.open(txt_file_name, "rb") as file:
        data = file.read().decode('utf-8')
        return data

######################################################


def get_items(txt_file_name):
    items = list(set(re.findall("Item information:\t\t(\w.*)",
                 get_text(txt_file_name), re.IGNORECASE)))
    return ";".join(items)


def search_append_items(df_append_items):
    with ThreadPoolExecutor(max_workers=1000) as executor:
        items = list(executor.map(get_items, list(df_append_items.path)))
    df_append_items["items"] = items
    return df_append_items

######################################################


def get_AGM_date(txt_file_name):
    agm_bool = bool(re.search("annual meeting of shareholders",
                    get_text(txt_file_name), re.IGNORECASE))
    return agm_bool


def search_append_date_AGMs(df_append_date_AGMs):
    with ThreadPoolExecutor(max_workers=1000) as executor:
        AGM_date = list(executor.map(
            get_AGM_date, list(df_append_date_AGMs.path)))

    df_append_date_AGMs["AGM_date"] = list(AGM_date)
    return df_append_date_AGMs

######################################################


def main():
    df = pd.read_csv("path_dataframe.csv", parse_dates=[
                     "dates"], dtype={"cik": str})
    df.drop(list(df.filter(regex='Unnamed')), axis=1, inplace=True)

    df.cik = df.cik.str.zfill(10)
    df["year"] = pd.DatetimeIndex(df['dates']).year

    # filter
    year = 2016
    year_filtered_df = df[df["year"] == year].drop_duplicates(
        subset=["serial"]).reset_index(drop=True)
    
    # sample
    sample_size = 100
    year_filtered_df_sample = year_filtered_df.sample(sample_size)
    # append items:
    start_item_time = time.time()
    year_filtered_df_with_items = search_append_items(
        year_filtered_df_sample).reset_index(drop=True)
    end_item_time = time.time()
    print("time for item extraction operation", end_item_time-start_item_time)

    # append date for AGMs:
    start_AGM_time = time.time()
    year_filtered_df_with_items_AGM_date = search_append_date_AGMs(
        year_filtered_df_with_items)
    end_AGM_time = time.time()
    print("time for AGM extraction operation", end_AGM_time-start_AGM_time)
    year_filtered_df_with_items_AGM_date.to_csv(
        f"OUTPUT/df_yfilter_sample_{sample_size}_{year}_with_items_AGM_date.csv")
    

def test():
    test = pd.read_csv("OUTPUT/df_yfilter_sample_100_2016_with_items_AGM_date.csv",
                       parse_dates=["dates"], dtype={"cik": str})
    test.drop(list(test.filter(regex='Unnamed')), axis=1, inplace=True)

    test.cik = test.cik.str.zfill(10)
    test["year"] = pd.DatetimeIndex(test['dates']).year

    print(test)


if __name__ == "__main__":
    main()
