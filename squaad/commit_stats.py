import numpy as np
import re


class ChunkStats:

	def __init__(self, chunk):
		self.removed = len(re.findall(r'\n\+', chunk))
		self.added = len(re.findall(r'\n-', chunk))
		self.changed = self.removed + self.added


class FileStats:
	def __init__(self, blob):
		self.blob = blob.diff.decode("utf-8", "ignore")
		self.change_type = blob.change_type
		self.chunks = [ChunkStats(x) for x in self.blob.split("\n@@ -")]
		self.added = sum(x.added for x in self.chunks)
		self.removed = sum(x.removed for x in self.chunks)
		self.changed = sum(x.changed for x in self.chunks)
		self.n_chunks = len(self.chunks)


class OverallStats:

	def __init__(self, stats, blobs):
		self.added = stats.total['insertions']
		self.removed = stats.total['deletions']
		self.changed = stats.total['lines']
		self.files = {}
		for blob in blobs:
			fname = blob.b_path if not blob.deleted_file else blob.a_path
			self.files[fname] = FileStats(blob)


class CommitStats:

	def __init__(self, c):
		self.csha = c.hexsha
		self.author = c.author.email
		self.authored_date = c.authored_datetime
		self.committer = c.committer.email
		self.committed_date = c.committed_datetime.replace(tzinfo=None)
		self.commit_msg = c.message
		blob = c.diff(c.parents[0], create_patch=True, **{"ignore-all-space": True})
		self.overall_stats = OverallStats(c.stats, blob)

	def entropy_lines_across_files(self):
		files = [[x.added, x.removed, x.changed] for x in filter(lambda y: not y.renamed, self.overall_stats.files.values())]
		file_line_counts = np.array(files)
		total = np.sum(file_line_counts, axis=0)
		g = np.nan_to_num(file_line_counts / total)
		h = np.sum(-1 * g * np.nan_to_num(np.log(g)), axis=0)
		return h

	def entropy_chunks_across_files(self):
		files = [len(x.chunks) for x in filter(lambda y: not y.renamed, self.overall_stats.files.values())]
		file_line_counts = np.array(files)
		total = np.sum(file_line_counts, axis=0)
		g = file_line_counts.T / total
		h = np.sum(-1 * g * np.nan_to_num(np.log(g)), axis=0)
		return h

	def entropy_lines_across_chunks(self):
		s = []
		for x in filter(lambda y: not y.renamed, self.overall_stats.files.values()):
			for y in x.chunks:
				s.append([y.added, y.removed, y.changed])
		file_line_counts = np.array(s)
		total = np.sum(file_line_counts, axis=0)
		g = file_line_counts / total
		h = np.sum(-1 * g * np.nan_to_num(np.log(g)), axis=0)
		return h

	def change_type_counts(self):
		counts = {'A': 0, 'R': 0, 'M': 0, 'D': 0}
		for f in self.overall_stats.files:
			counts[f.change_type] += 1
		return counts
