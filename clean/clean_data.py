import pandas as pd 
import numpy as np 

df = pd.read_csv("../user_journey_raw.csv", delimiter=",")

def remove_page_duplicates(df, target_column='user_journey'):
    """
    Removes sequences of sequential duplicate pages from the specified column
    in the dataframe and returns a new dataframe with cleaned-up journey strings.
    
    Parameters:
        data (pd.DataFrame): The dataframe containing the data.
        target_column (str): The name of the column with the user journey strings.
                              Defaults to 'user_journey'.
    
    Returns:
        pd.DataFrame: A new dataframe with the cleaned-up journey strings.
    """
    # Create a copy to avoid modifying the original dataframe
    df_clean = df.copy()

    def remove_duplicates(journey):
        # If the journey is not a string, return it as-is
        if not isinstance(journey, str):
            return journey
        
        # Split the string by hyphen to get the pages
        pages = journey.split('-')
        if not pages:
            return journey
        
        # Initialize the cleaned list with the first page
        cleaned_pages = [pages[0]]
        # Iterate over the remaining pages and only add if different from the last one
        for page in pages[1:]:
            if page != cleaned_pages[-1]:
                cleaned_pages.append(page)
        # Join the cleaned list back into a string
        return '-'.join(cleaned_pages)

    # Apply the cleaning function to the target column
    df_clean[target_column] = df_clean[target_column].apply(remove_duplicates)
    
    return df_clean



def group_by(data, group_column='user_id', target_column='user_journey', 
             session_id="session_id", subscription_type="subscription_type", 
             sessions='All', count_from='last'):
    """
    Groups the user journey strings into one big string per group (user),
    while preserving session_id and subscription_type.

    Parameters:
        data (pd.DataFrame): The dataframe containing all the data.
        group_column (str): The column to group by (e.g., 'user_id').
        target_column (str): The column containing the user journey strings.
        session_id (str): The column containing session IDs.
        subscription_type (str): The column containing subscription info.
        sessions (int or str): Number of sessions to group ('All' for all sessions, else integer).
        count_from (str): 'first' or 'last' (if sessions is an integer, determines whether to keep first/last N sessions).

    Returns:
        pd.DataFrame: A new dataframe with grouped user journeys, session IDs, and subscription types.
    """
    grouped_records = []

    # Sort by session_id to ensure order is correct before grouping
    data = data.sort_values(by=[group_column, session_id])

    # Group by user_id
    grouped = data.groupby(group_column, sort=False)

    for group_name, group_df in grouped:
        # Select subset of sessions
        if sessions == 'All':
            subset = group_df
        else:
            try:
                session_count = int(sessions)
            except ValueError:
                raise ValueError("The sessions parameter must be an integer or 'All'.")
            
            if count_from.lower() == 'first':
                subset = group_df.head(session_count)
            elif count_from.lower() == 'last':
                subset = group_df.tail(session_count)
            else:
                raise ValueError("count_from must be either 'first' or 'last'.")

        # Combine user journey pages
        grouped_journey = '-'.join(subset[target_column].tolist())

        # Combine session IDs
        grouped_sessions = '-'.join(subset[session_id].astype(str).tolist())

        # Combine unique subscription types
        grouped_subscription = '-'.join(subset[subscription_type].astype(str).unique())

        # Append new grouped record
        grouped_records.append({
            group_column: group_name,
            session_id: grouped_sessions,
            subscription_type: grouped_subscription,
            target_column: grouped_journey
        })

    # Return the new dataframe
    return pd.DataFrame(grouped_records)

cleaned_df = remove_page_duplicates(df)

user_id = group_by(cleaned_df)

cleaned_df = remove_page_duplicates(user_id)

cleaned_df.to_csv("cleaned_df.csv", index=False)
