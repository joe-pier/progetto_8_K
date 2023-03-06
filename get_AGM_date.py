import pandas as pd

def main():
    # filter:
    AGM_bool_true = AGM_bool[AGM_bool["AGM"] == True].reset_index(drop = True)
    print(AGM_bool_true)
    return AGM_bool_true

def get_AGM_date():
    """ get the agm date in the documents that contain references of the AGM """
    return

if __name__ == "__main__":
    year = 2016
    sample_size = 2000
    try:
        AGM_bool = pd.read_csv(f".OUTPUT/AGMs_excomp_y{year}_s{sample_size}.csv")
        AGM_bool.drop(list(AGM_bool.filter(regex='Unnamed')), axis=1, inplace=True)
    except:
        pass
    finally:
        main()