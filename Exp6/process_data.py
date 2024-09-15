from hdfs import InsecureClient
import pandas as pd
import json

# Function to read JSON data from HDFS
def read_json_from_hdfs(client, hdfs_path):
    try:
        with client.read(hdfs_path, encoding='utf-8') as reader:
            json_data = reader.read()
            if not json_data.strip():
                raise ValueError("The JSON file is empty.")
            return json.loads(json_data)
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        exit(1)
    except Exception as e:
        print(f"Error reading or parsing JSON data: {e}")
        exit(1)

# Function to convert JSON data to DataFrame
def json_to_dataframe(data):
    try:
        return pd.DataFrame(data)
    except ValueError as e:
        print(f"Error converting JSON data to DataFrame: {e}")
        exit(1)

# Function to save DataFrame as JSON back to HDFS
def save_json_to_hdfs(client, hdfs_path, data):
    try:
        with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
            writer.write(data)
        print("Filtered JSON file saved successfully.")
    except Exception as e:
        print(f"Error saving filtered JSON data: {e}")
        exit(1)

# Main function to perform operations
def main():
    # Connect to HDFS
    hdfs_client = InsecureClient('http://localhost:9870', user='harish')

    # Read JSON data from HDFS
    data = read_json_from_hdfs(hdfs_client, '/exp6/emp.json')

    # Convert JSON data to DataFrame
    df = json_to_dataframe(data)

    # Projection: Select only 'name' and 'salary' columns
    projected_df = df[['name', 'salary']]

    # Aggregation: Calculate total salary
    total_salary = df['salary'].sum()

    # Count: Number of employees earning more than 50000
    high_earners_count = df[df['salary'] > 50000].shape[0]

    # Limit: Get the top 5 highest earners
    top_5_earners = df.nlargest(5, 'salary')

    # Skip: Skip the first 2 employees
    skipped_df = df.iloc[2:]

    # Remove: Remove employees from the 'IT' department
    filtered_df = df[df['department'] != 'IT']

    # Save the filtered result back to HDFS
    filtered_json = filtered_df.to_json(orient='records')
    save_json_to_hdfs(hdfs_client, '/home/exp6/filtered_employees.json', filtered_json)

    # Print results
    print("Projection: Select only 'name' and 'salary' columns")
    print(projected_df)
    print("\nAggregation: Total salary of all employees")
    print(f"Total Salary: {total_salary}")
    print(f"\nCount: Number of employees earning more than 50000")
    print(f"Number of High Earners (>50000): {high_earners_count}")
    print(f"\nTop 5 Earners:")
    print(top_5_earners)
    print(f"\nSkipped DataFrame (First 2 rows skipped):")
    print(skipped_df)
    print(f"\nFiltered DataFrame (IT department removed):")
    print(filtered_df)

# Run the main function
if __name__ == '__main__':
    main()
