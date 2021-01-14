# create pandas df with time information
# pdf = pd.DataFrame([[car_id, timestamp_millis, year, month, day]],
#                    columns=["carId", "timestamp", "year", "month", "day"])

# persist processed data as parquet
#flatten = flatten_json(mapped_data)
# pdf = pd.DataFrame(flatten, index=[0])
# if not io.write_partitioned_parquet_from_pandas(pdf, region[c.PROCESSED], ["year", "month", "day"]):
#     print(
#         f"Failed to persist {c.PROCESSED} data to {region[c.PROCESSED]}!")

# persist preprocessed data as parquet
# pdf = pd.DataFrame.from_dict(data)
# if not io.write_partitioned_parquet_from_pandas(pdf, region[c.PREPROCESSED], ["year", "month", "day"]):
#     print(
#         f"Failed to persist {c.PREPROCESSED} data to {region[c.PREPROCESSED]}!")
