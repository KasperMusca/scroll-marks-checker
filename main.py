import requests
import pandas as pd
from datetime import datetime
import os

def read_wallet_addresses(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

wallet_addresses_file = 'wallet_addresses.txt'
wallet_addresses = read_wallet_addresses(wallet_addresses_file)

api_endpoint = 'https://kx58j6x5me.execute-api.us-east-1.amazonaws.com/scroll/wallet-points?walletAddress='

def fetch_points(wallet_address):
    response = requests.get(f'{api_endpoint}{wallet_address}')
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for wallet {wallet_address}: Status code {response.status_code}")
        return None

def extract_points(data, wallet_address):
    records = []
    if data:
        for entry in data:
            points = entry.get('points', 0)
            timestamp = entry.get('timestamp', 'Unknown')
            records.append({
                'wallet_address': wallet_address,
                'points': points,
                'timestamp': datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
            })
    return records

all_records = []

for wallet_address in wallet_addresses:
    data = fetch_points(wallet_address)
    records = extract_points(data, wallet_address)
    all_records.extend(records)

df = pd.DataFrame(all_records)
df_aggregated = df.groupby('wallet_address')['points'].sum().reset_index()
total_points = df['points'].sum()

print("Aggregated points per wallet:")
print(df_aggregated)
print(f"\nTotal points across all wallets: {total_points}")

output_folder = 'wallet_points_snapshots'
os.makedirs(output_folder, exist_ok=True)

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
csv_file_name = os.path.join(output_folder, f'aggregated_points_per_wallet_{timestamp}.csv')
df_aggregated.to_csv(csv_file_name, index=False)

print(f"Aggregated points per wallet saved to {csv_file_name}")
