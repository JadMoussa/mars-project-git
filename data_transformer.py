import pandas as pd
dataframe = pd.read_csv('your_data.csv')
def transform_data(dataframe, transformation_type, **kwargs):
    if transformation_type == 'filter':
        return dataframe.query(**kwargs)
    elif transformation_type == 'sort':
        return dataframe.sort_values(**kwargs)
    elif transformation_type == 'groupby':
        return dataframe.groupby(**kwargs)
    # Add more transformation types as needed.
    else:
        raise ValueError("Unsupported transformation type: " + transformation_type)

# Example usage:
# Filter data in a DataFrame
filtered_data = transform_data(dataframe, 'filter', expr="column_name > 5")

# Sort data in a DataFrame
sorted_data = transform_data(dataframe, 'sort', by="column_name", ascending=False)

# Group data in a DataFrame
grouped_data = transform_data(dataframe, 'groupby', by="column_name")
