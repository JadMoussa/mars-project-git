import pandas as pd

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
