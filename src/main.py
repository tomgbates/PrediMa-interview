import pandas as pd
import requests


def main():
    """Main function to run

    This function ingests NASA data from a local JSON file in ./data directory.
    This will calculate the average mass of the meteriorites discovered, grouped by century, and the post this
    data to https://webhook-test.com/c4319e2456190a0aa5cbbf326f959a77

    Args:
        None.
    
    Returns:
        response (Response): The response of posting the payload (average mass of the meteors per century series)
    """
    # Load the data
    df = pd.read_json("./data/nasa_data.json")

    # Clean data. Remove unnecessary columns
    columns_to_delete = [
        "id",
        "nametype",
        "recclass",
        "fall",
        "reclat",
        "reclong",
        "geolocation",
        ":@computed_region_cbhk_fwbd",
        ":@computed_region_nnqa_25f4"
    ]

    cleaned_df = df.drop(columns=columns_to_delete)

    # Covert the year column to timestamp
    cleaned_df["year"] = pd.to_datetime(cleaned_df["year"],  format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce')

    # Group data by the centuries
    cleaned_df["century"] = (cleaned_df["year"].dt.year // 100) * 100

    # group the data by century
    average_mass = cleaned_df.groupby("century")["mass"].mean()

    average_mass_data = average_mass.to_dict()

    # post the data
    endpoint = "https://webhook-test.com/c4319e2456190a0aa5cbbf326f959a77"
    response = requests.post(url=endpoint, json=average_mass_data)

    if response.status_code == 200:
        print("Success")
    else:
        print(response.status_code)

    return response


if __name__ == "__main__":
    main()
