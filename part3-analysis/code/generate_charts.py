import matplotlib.pyplot as plt
import seaborn as sns

# Chart 1: Attribution Coverage (Pie)
labels = ['Unattributed', '175c1c8e', 'Other Tagged']
sizes = [226461, 13500, 54500]
colors = ['#ff6b6b', '#4ecdc4', '#95e1d3']
plt.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors)
plt.title('Revenue Attribution Coverage\n(77% Unmeasurable)')
plt.savefig('attribution_coverage.png')

# Chart 2: Funnel (Horizontal Bar)
stages = ['Site Visitors', 'Add to Cart', 'Checkout', 'Purchase']
counts = [47821, 2391, 842, 294]
plt.barh(stages, counts)
plt.title('Conversion Funnel (14-Day Period)')
plt.xlabel('Events')
plt.savefig('conversion_funnel.png')

# Chart 3: Data Quality Timeline
import pandas as pd
dates = pd.date_range('2025-02-23', '2025-03-08')
valid_client_id = [100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
plt.plot(dates, valid_client_id, marker='o')
plt.axvline(pd.Timestamp('2025-02-27'), color='r', linestyle='--', label='Schema Drift')
plt.title('Data Quality: client_id Completeness Over Time')
plt.ylabel('% with Valid ID')
plt.legend()
plt.savefig('data_quality_timeline.png')