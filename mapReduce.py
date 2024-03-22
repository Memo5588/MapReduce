# Import necessary libraries for the MapReduce implementation.
import csv
from collections import defaultdict
import heapq

# Data preprocessing phase  - the provided file is read and processed to extract passenger IDs.
def preprocess_data(file_path):
    processed_data = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            # Assuming the data format is consistent with provided instructions
            passenger_id = row[0]
            processed_data.append(passenger_id)
    return processed_data

# Map Phase - the data is processed to generate intermediate key-value pairs.
def mapper(data):
    mapped_result = defaultdict(int)
    for passenger_id in data:
        mapped_result[passenger_id] += 1
    return mapped_result.items()

# Shuffle Phase - redistributes the intermediate key-value pairs to prepare them for the reduce phase.
def shuffle(mapped_result):
    shuffled_result = defaultdict(list)
    for key, value in mapped_result:
        hashed_key = hash(key) % 10  # Assuming 10 reducers for simplicity
        shuffled_result[hashed_key].append((key, value))
    return shuffled_result.items()

# Reduce Phase  -  the shuffled data is aggregated to produce the final result.
def reducer(shuffled_result):
    reduced_result = defaultdict(int)
    for _, key_value_list in shuffled_result:
        for passenger_id, count in key_value_list:
            reduced_result[passenger_id] += count
    return reduced_result

# Output Formatting  - the format_output function formats and displays the final output.
def format_output(result, n=1):
    top_passengers = heapq.nlargest(n, result.items(), key=lambda x: x[1])
    for passenger_id, count in top_passengers:
        print(f"Passenger ID: {passenger_id} - Total Flights: {count}")

if __name__ == "__main__":
    # Data preprocessing
    flight_data = preprocess_data("AComp_Passenger_data_no_error.csv")
    
    # Map Phase
    mapped_result = mapper(flight_data)
    
    # Shuffle Phase
    shuffled_result = shuffle(mapped_result)
    
    # Reduce Phase
    reduced_result = reducer(shuffled_result)
    
    # Output formatting
    format_output(reduced_result)