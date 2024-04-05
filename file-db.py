#!/usr/bin/env python3
import concurrent.futures
import functools
import hashlib
import itertools
import os
import sys
import time

def progressbar(it, prefix="", size=60, out=sys.stdout): # Python3.6+
    count = len(it)
    start = time.time()
    def show(j):
        x = int(size*j/count)
        remaining = ((time.time() - start) / j) * (count - j)

        mins, sec = divmod(remaining, 60)
        time_str = f"{int(mins):02}:{sec:05.2f}"

        print(f"{prefix}[{u'█'*x}{('.'*(size-x))}] {j}/{count} Est wait {time_str}", end='\r', file=out, flush=True)

    for i, item in enumerate(it):
        yield item
        show(i+1)
    print("\n", flush=True, file=out)


def get_digest(path):
    with open(path, 'rb') as fd:
        digest = hashlib.file_digest(fd, 'sha256')
        hex_digest = digest.hexdigest()
    return hex_digest


class FileNode:

    def __init__(self, path: str, digest_executor: concurrent.futures.Executor):
        if path is None or path == "":
            raise Exception("directory path should not be empty")
        self.path = path
        self._digest = digest_executor.submit(get_digest, path)
        # self._digest.add_done_callback(lambda f: print(f.result() + " " + self.path))
        self._digest.add_done_callback(lambda f: self._add_to_digests(f.result()))
        futures.append(self._digest)

    def _add_to_digests(self, digest):
        if digest not in digests:
            digests[digest] = {self}
        else:
            digests[digest].add(self)

    def digest(self):
        return self._digest.result()


class DirectoryNode:
    def __init__(self, path):
        if path is None or path == "":
            raise Exception("directory path should not be empty")
        self.path = path
        self._file_nodes = {}
        self._directory_nodes = {}
        self.paths = set()

    def add_file(self, file: FileNode):
        self._file_nodes[file.path] = file
        self.paths.add(file.path)

    def add_directory(self, directory):
        self._directory_nodes[directory.path] = directory
        self.paths.add(directory.path)

    def compare(self, other):
        intersection = set.intersection(self.paths, other.paths)
        intersection_length = len(intersection)
        num_paths = len(self.paths)

        return intersection_length/num_paths


class TreeNode:
    def __init__(self, tree, name: str, sub_dirs: list[str], files: list[str], is_dir: bool, digest_executor: concurrent.futures.Executor):
        self.tree: dict[str, TreeNode] = tree
        self.name = name
        self.is_dir = is_dir
        if is_dir:
            self.sub_dirs = [os.path.join(name, sub_dir) for sub_dir in sub_dirs]
            self.files = [os.path.join(name, file) for file in files]
            self.sub_dirs.sort()
            self.files.sort()
        else:
            self.sub_dirs = None
            self.files = None
        self.digest_executor = digest_executor

        if is_dir:
            self._digest = concurrent.futures.Future()
        else:
            self._digest = digest_executor.submit(get_digest, name)
            self._digest.add_done_callback(lambda f: print(f.result() + " " + self.name))
            self._digest.add_done_callback(lambda f: self._add_to_digests(f.result()))
            futures.append(self._digest)

    def _add_to_digests(self, digest):
        if digest not in digests:
            digests[digest] = {self}
        else:
            digests[digest].add(self)

    def digest(self):
        if self.is_dir and not self._digest.done():
            sub_dir_nodes = map(lambda path: self.tree[path], self.sub_dirs)
            sub_dir_node_digests = map(lambda node: node.digest(), sub_dir_nodes)

            file_nodes = map(lambda path: self.tree[path], self.files)
            file_node_digests = map(lambda node: node.digest(), file_nodes)

            all_digests = itertools.chain(sub_dir_node_digests, file_node_digests)
            digest = functools.reduce(lambda a, b: hashlib.sha256(str(a + b).encode('utf-8')).hexdigest(), all_digests, '')

            self._digest.set_result(digest)
            self._add_to_digests(digest)

        return self._digest.result()

    def report(self):
        dupes = digests[self.digest()]
        dupes = dupes.copy()
        dupes = list(dupes)
        dupes.sort()
        dupes = tuple(dupes)
        dupe_groups.add(dupes)

        if self.is_dir:
            for sub_dir in self.sub_dirs:
                self.tree[sub_dir].report()
            for file in self.files:
                self.tree[file].report()

    def __hash__(self):
        return self.name.__hash__()

    def __eq__(self, other):
        return self.name == other.path

    def __lt__(self, other):
        return self.name < other.path


futures = []
digests: dict[str, set[FileNode]] = {}
file_digests: dict[str, set[FileNode]] = {}
dupe_groups: set[tuple[TreeNode, ...]] = set()
tree = {}


def main(root_dir):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        tree[root_dir] = DirectoryNode(root_dir)
        for curr_dir, sub_dirs, files in os.walk(root_dir):
            curr_dir_node = tree[curr_dir]
            files.sort()
            sub_dirs.sort()
            for file in files:
                path = os.path.join(curr_dir, file)
                sub_file_node = FileNode(path, executor)
                curr_dir_node.add_file(sub_file_node)
                tree[path] = sub_file_node
            for sub_dir in sub_dirs:
                path = os.path.join(curr_dir, sub_dir)
                sub_dir_node = DirectoryNode(path)
                curr_dir_node.add_directory(sub_dir_node)
                tree[path] = sub_dir_node
        print("Directory walk complete, waiting for file digests to be calculated...")
        print(f"{str(len(futures))} digests to calculate")
        while len(digests) < len(futures):
            print(f"{len(digests)/len(futures)*100}% done")
            time.sleep(1)
        concurrent.futures.wait(futures, None, )
        tree["/home/wheeler/Documents/Old Stuff/keep/Old Music"].compare(tree["/home/wheeler/Documents/Old Laptop Backup/Music"])
        print("Done!")

        # print("Report:")
        # tree[root_dir].report()
        # dupe_group_list = list(dupe_groups)
        # dupe_group_list.sort()
        # for dupe_group in dupe_group_list:
        #     first = dupe_group[0]
        #     if first.is_dir:
        #         prefix = 'd'
        #     else:
        #         prefix = 'f'
        #
        #     if len(dupe_group) == 1:
        #         print(prefix + " ━━ " + first.name)
        #     else:
        #         print(prefix + " ┳━ " + first.name)
        #         for dupe in dupe_group[1:-1]:
        #             print("  ┣━ " + dupe.name)
        #         print("  ┗━ " + dupe_group[-1].name)


main('/home/wheeler/Documents')
