from qumulo.rest_client import RestClient
import os
import ssl
import heapq
from optparse import OptionParser
from multiprocessing import Pool

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

class SampleTreeNode:
    def __init__(self, name, parent=None):
        self.parent = parent
        self.samples = 0
        self.name = name
        self.sum_samples = 0
        self.children = {}

    def insert(self, name, samples):
        self.insert_internal(name.split("/"), samples)

    def insert_internal(self, components, samples):
        if not components:
            self.samples += samples
        else:
            self.children.setdefault(components[0], SampleTreeNode(components[0], self))
            self.children[components[0]].insert_internal(components[1:], samples)
        self.sum_samples += samples

    def leaves(self):
        if not self.children:
            yield self
        for child in self.children.values():
            for result in child.leaves():
                yield result

    def merge_up(self):
        if not self.parent:
            return self
        self.parent.samples += self.samples
        del self.parent.children[self.name]
        return self.parent

    def prune_until(self, max_leaves=10, min_samples=5):
        leaves = []
        for leaf in self.leaves():
            leaves.append((leaf.samples, leaf))

        heapq.heapify(leaves)

        while leaves[0][1].parent:
            lowest = heapq.heappop(leaves)
            if lowest[0] > min_samples and len(leaves) < max_leaves:
                break
            new_node = lowest[1].merge_up()
            if len(new_node.children) == 0:
                heapq.heappush(leaves, (new_node.samples, new_node))

    def __str__(self, indent, format_samples, is_last=True):
        result = indent + (is_last and "\\---" or "+---") + self.name + ""
        if self.samples:
            result += "(%s)" % (format_samples(self.sum_samples),)

        next_indent = indent + (is_last and "    " or "|   ")
        sorted_children = sorted(self.children.values(),
                                 lambda x, y: cmp(x.name, y.name))
        for child in sorted_children[:-1]:
            result += "\n" + child.__str__(
                next_indent, format_samples, False)
        if sorted_children:
            result += "\n" + sorted_children[-1].__str__(
                next_indent, format_samples, True)

        return result

def pretty_print_capacity(x):
    start = (1024 ** k for k in (6, 5, 4, 3, 2, 1, 0))
    units = ("E", "P", "T", "G", "M", "K", "b")
    for l, u in zip(start, units):
        if x >= l: return "%0.02f%s" % (x / float(l), u)
    return 0

def get_samples_worker(x):
    credentials, path, n = x
    client = RestClient(credentials["cluster"], 8000)
    client.login(credentials["user"], credentials["password"])
    return client.fs.get_file_samples(path=path, count=n, by_value="capacity")

class memoize:
  def __init__(self, function):
    self.function = function
    self.memoized = {}
  def __call__(self, *args):
    try:
      return self.memoized[args]
    except KeyError:
      self.memoized[args] = self.function(*args)
      return self.memoized[args]

def format_owner(identities):
    preferred_keys = ('LOCAL_USER', 'NFS_UID')
    for key in preferred_keys:
        for el in identities:
            if el['id_type'] == key:
                return el["id_type"] + ":" + el["id_value"]
    return "ERROR"

@memoize
def translate_owner_to_owner_string(cli, owner):
    return format_owner(cli.auth.auth_id_to_all_related_identities(owner))

seen = {}
def get_file_attrs(x):
    credentials, paths = x
    client = RestClient(credentials["cluster"], 8000)
    client.login(credentials["user"], credentials["password"])
    result = []
    for path in paths:
        if seen.has_key(path):
            result += [seen[path]]
            continue
        owner_id = client.fs.get_attr(path)["owner"]
        str_owner = translate_owner_to_owner_string(client, owner_id)
        seen[path] = str_owner
        result.append(str_owner)
    return result

def get_samples(pool, path, credentials, opts):
    return sum(pool.map(
        get_samples_worker,
        ([(credentials, args[0], opts.samples / opts.concurrency)] * opts.concurrency)),
                  [])

def get_owner_vec(pool, credentials, samples, opts):
    file_ids = [s["id"] for s in samples]
    sublists = [(credentials, file_ids[i:i+100]) for i in xrange(0, opts.samples, 100)]
    owner_id_sublists = pool.map(get_file_attrs, sublists)
    return sum(owner_id_sublists, [])

def do_it(opts, args):
    credentials = {"user" : opts.user,
                   "password" : opts.password,
                   "cluster" : opts.cluster}

    # Qumulo API login
    client = RestClient(opts.cluster, opts.port)
    client.login(opts.user, opts.password)

    total_capacity_used = int(
        client.fs.read_dir_aggregates(args[0])['total_capacity'])

    pool = Pool(opts.concurrency)

    # First build a vector of all samples...
    samples = get_samples(pool, args[0], credentials, opts)

    # Then get a corresponding vector of owner strings
    owner_vec = get_owner_vec(pool, credentials, samples, opts)

    owners = {}
    directories = {}

    # Create a mapping of user to tree...
    for s, owner in zip(samples, owner_vec):
        owners.setdefault(owner, SampleTreeNode(""))
        owners[owner].insert(s["name"], 1)

    def format_capacity(samples):
        bytes_per_terabyte = 1000. ** 4
        if opts.dollars_per_terabyte != None:
            return "$%0.02f/month" % (samples * total_capacity_used /
                                      opts.samples /
                                      bytes_per_terabyte *
                                      opts.dollars_per_terabyte)
        else:
            return pretty_print_capacity(samples * total_capacity_used / opts.samples)

    print "Total: %s" % (format_capacity(opts.samples))
    sorted_owners = sorted(owners.items(),
                           lambda x, y: cmp(y[1].sum_samples, x[1].sum_samples))
    # For each owner, print total used, then refine the tree and dump it.
    for name, tree in sorted_owners:
        print "Owner %s (~%0.1f%%/%s)" % (
            name, tree.sum_samples / float(opts.samples) * 100,
            format_capacity(tree.sum_samples))
        tree.prune_until(max_leaves=opts.max_leaves,
                         min_samples=opts.min_samples)

        print tree.__str__("    ", lambda x: format_capacity(x))

def process_command_line():
    usage = "usage: %prog [options] path"
    parser = OptionParser(usage=usage)
    parser.add_option(
        "-U", "--user",
        help="The user to connect as",
        action="store", dest="user", type="string", default="admin")

    parser.add_option(
        "-P", "--password",
        help="The password to connect with",
        action="store", dest="password", type="string", default="admin")

    parser.add_option(
        "-C", "--cluster",
        help="The cluster to connect to",
        action="store", dest="cluster", type="string", default="qumulo")

    parser.add_option(
        "-p", "--port",
        help="The port to connect to",
        action="store", dest="port", type="int", default=8000)

    parser.add_option(
        "-s", "--samples",
        help="The number of samples to take", default=2000,
        action="store", dest="samples", type="int")

    parser.add_option(
        "-c", "--concurrency",
        help="The number of threads to query with", default=10,
        action="store", dest="concurrency", type="int")

    parser.add_option(
        "-m", "--min-samples",
        help="The minimum number of samples to show at a leaf in output",
        action="store", dest="min_samples", type="int", default=5)

    parser.add_option(
        "-x", "--max-leaves",
        help="The maximum number of leaves to show per user",
        action="store", dest="max_leaves", type="int", default=30)

    parser.add_option(
        "-D", "--dollars-per-terabyte",
        help="Show capacity in dollars. Set conversion factor in $/TB/month",
        action="store", dest="dollars_per_terabyte", type="float", default=None)

    (opts, args) = parser.parse_args()

    return opts, args

(opts, args) = process_command_line()
do_it(opts, args)
