import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('logs/batchingestion_metrics.log')

df = df[df['success']=='True']
df = df[~df['concurrent_ingestions'].isna()]

# parse ingestion_time to a timedelta object
df['ingestion_time'] = pd.to_timedelta(df['ingestion_time'])

# calculate throughput using the ingestion_time and the file_size
df['throughput'] = df['file_size'] / df['ingestion_time'].dt.total_seconds()

# map throughput to MB/s
df['throughput'] = df['throughput'] / (1024 * 1024)

# plot throughput vs concurrent_ingestions
plt.scatter(df['concurrent_ingestions'], df['throughput'], alpha=0.5)
plt.xlabel('Concurrent Ingestions')
plt.ylabel('Throughput (MB/s)')
plt.xticks([1, 2, 4, 8, 12, 16, 20, 24])

plt.savefig('reports/throughput_vs_concurrent_ingestions.png')

mean = df.groupby('concurrent_ingestions')['throughput'].mean()
total = mean * mean.index
print(total.max(), total.mean(), total.std())

# plot total throughput vs concurrent_ingestions
plt.bar(total.index, total)
plt.xlabel('Concurrent Ingestions')
plt.ylabel('Total Throughput (MB/s)')
plt.savefig('reports/total_throughput_vs_concurrent_ingestions.png')

df = pd.read_csv('logs/batchingestion_metrics.log')

# ratio of successful to failed ingestions per tenant
df.loc[~(df['success'] == 'True'), 'success'] = 'False'
df = df.groupby('tenant')['success'].value_counts().unstack().fillna(0)

print(df)

