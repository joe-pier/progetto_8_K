import pandas as pd
import csv
import requests
import shutil
import os


def get_def14a_year_qrt(year, qrt):
    df = pd.read_csv(f".masterfiles\master_{year}_{qrt}\master_{year}_{qrt}.csv", header=
                     [0], delimiter="|", quoting=csv.QUOTE_NONE, encoding='utf-8', index_col=0)

    df = df[df['Company Name'].notna()].reset_index(drop=True)

    df = df.drop(df.filter(regex="Unnamed"), axis=1).rename(
        {'"CIK': "cik", "Company Name": "CoName", "Form Type": "ftype", "Date Filed": "dfiled", 'Filename"': "fname"}, axis=1)

    df["cik"] = df.cik.str.replace('""', "").str.zfill(10)

    return df[df["ftype"] == "DEF 14A"].reset_index(drop=True)


def retrive_documents(df, year, qrt):
    
    os.mkdir(f"DEF14A_{year}_{qrt}")
    for n, company in enumerate(df['fname']):
        print(n, len(df),df['fname'][n])
        url = "https://www.sec.gov/Archives/edgar/data/1013706/0001171843-17-002290.txt"
        r = requests.get(url+company, stream=True,
                         headers={'User-agent': 'Mozilla/5.0'})
        print(r.status_code)
        if r.status_code == 200:
            with open(f"DEF14A_{year}_{qrt}/"+'serial__'+str(n)+"__CIK__"+company[11:18].replace("/", "_")+'__date__'+list(df['dfiled'])[n]+'.txt', 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)


if __name__ == "__main__":
    year, qrt = [2017, "QTR3"]

    df = get_def14a_year_qrt(year, qrt)
    retrive_documents(df, year, qrt)
