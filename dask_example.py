# Note that this code must be run from python3 or ipython3 in a session's 
# terminal, not run directly in the graphical console. See
# https://github.com/dask/dask/issues/4612

import cdsw_dask_utils 
import cdsw_await_workers

# Run a Dask cluster with three workers and return an object containing
# a description of the cluster. 
# 
# Note that the scheduler will run in the current session, and the Dask
# dashboard will become available in the nine-dot menu at the upper
# right corner of the CDSW app.

cluster = cdsw_dask_utils.run_dask_cluster(
  n=2, \
  cpu=1, \
  memory=1, \
  nvidia_gpu=0
)

# Connect a Dask client to the scheduler address in the cluster
# description.
from dask.distributed import Client
client = Client(cluster["scheduler_address"])

def square(x):
  return x ** 2

def neg(x):
  return -x

A = client.map(square, range(10))
print(client.gather(A))

B = client.map(neg, A)
print(client.gather(B))

total = client.submit(sum, B)
print("Result: ", total.result())