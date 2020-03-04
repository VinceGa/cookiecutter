from .hash_str import get_csci_salt, get_user_id, hash_str
from .io import atomic_write
import pandas as pd
import os

def get_user_hash(username, salt=None): #pragma: no cover
    salt = salt or get_csci_salt()
    return hash_str(username, salt=salt)


def convert_excel_to_parquet(source):
        """Converts the  excel file to the parquet format

        :param source: the excel file
        :return: the converted  parquet file
        """
        # read excel file into dataframe
        df = pd.read_excel(source, index_col=0)

        # save dataframe to parquet file
        parquet_file = os.path.splitext(source)[0] + ".parquet"
        with atomic_write(parquet_file, as_file=False) as f:
            df.to_parquet(f, engine="pyarrow")

        # return parquet file path
        return parquet_file

def read_parquet_columns(parquet_file, columns):
    """read columns from the parquet file

    :param parquet_file: path to parquet file
    :param columns: list of columns
    :return: dataframe containing requested columns only. Note that we used pyarrow engine
    as the default.
    """
    # read specified columns and return them
    data = pd.read_parquet(parquet_file, engine="pyarrow", columns=columns)
    return data

if __name__ == "__main__":  #pragma: no cover

    for user in ["gorlins", "vincega"]:
        print("Id for {}: {}".format(user, get_user_id(user)))

    data_source = "data/hashed.xlsx"

    parquet_file = convert_excel_to_parquet(data_source)

    print(parquet_file)
    #data=pd.read_parquet(parquet_file, engine="pyarrow", columns=["hashed_id"])
    print("Start printing...")
    print(read_parquet_columns(parquet_file, ["hashed_id"]))
    


