# Capacity By User
## What's the point?
This script uses the sampling facilities built into Qumulo Core to provide an overview of capacity consumption first by user, and then by area of the tree.

## Scenario
Imagine you have a hundred or a thousand artists or researchers all busily collaborating on a project. They create data, and sometimes they forget to go clean it up. You want to get your army of talented professionals to reduce their capacity consumption. What do you tell them? This script breaks down a tree by user and then shows, for each user, where that user consumes space in the tree.

## Usage
```
Usage: capacity_by_user.py [options] path

Options:
  -h, --help            show this help message and exit
  -U USER, --user=USER  The user to connect as
  -P PASSWORD, --password=PASSWORD
                        The password to connect with
  -C CLUSTER, --cluster=CLUSTER
                        The cluster to connect to
  -p PORT, --port=PORT  The port to connect to
  -s SAMPLES, --samples=SAMPLES
                        The number of samples to take
  -c CONCURRENCY, --concurrency=CONCURRENCY
                        The number of threads to query with
  -m MIN_SAMPLES, --min-samples=MIN_SAMPLES
                        The minimum number of samples to show at a leaf in
                        output
  -x MAX_LEAVES, --max-leaves=MAX_LEAVES
                        The maximum number of leaves to show per user
  -D DOLLARS_PER_TERABYTE, --dollars-per-terabyte=DOLLARS_PER_TERABYTE
                        Show capacity in dollars. Set conversion factor in
                        $/TB/month
```

