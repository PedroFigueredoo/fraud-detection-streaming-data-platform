# Install dependencies as needed:
# pip install kagglehub[pandas-datasets]
import kagglehub
from kagglehub import KaggleDatasetAdapter

# Set the path to the file you'd like to load
file_path = "/Users/pedrofigueredo/.cache/kagglehub/datasets/sgpjesus/bank-account-fraud-dataset-neurips-2022/versions/2"

# Load the latest version
df = kagglehub.load_dataset(
  KaggleDatasetAdapter.PANDAS,
  "sgpjesus/bank-account-fraud-dataset-neurips-2022",
  file_path,
  # Provide any additional arguments l# Install dependencies as needed:
    # # pip install kagglehub[pandas-datasets]
    # import kagglehub
    # from kagglehub import KaggleDatasetAdapter
    #
    # # Set the path to the file you'd like to load
    # file_path = ""
    #
    # # Load the latest version
    # df = kagglehub.load_dataset(
    #   KaggleDatasetAdapter.PANDAS,
    #   "sgpjesus/bank-account-fraud-dataset-neurips-2022",
    #   file_path,
    #   # Provide any additional arguments like
    #   # sql_query or pandas_kwargs. See the
    #   # documenation for more information:
    #   # https://github.com/Kaggle/kagglehub/blob/main/README.md#kaggledatasetadapterpandas
    # )
    #
    # print("First 5 records:", df.head())ike
  # sql_query or pandas_kwargs. See the
  # documenation for more information:
  # https://github.com/Kaggle/kagglehub/blob/main/README.md#kaggledatasetadapterpandas
)

print("First 5 records:", df.head())


