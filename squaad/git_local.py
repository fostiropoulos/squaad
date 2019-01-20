import tempfile
from git import Repo, Commit
from git.util import hex_to_bin
import git.repo.fun as repo_funcs
import os


class GitRepo(object):

	def __init__(self, root_dir=None):

		self.repo = None

		if root_dir and os.path.exists(root_dir):
			self.root = root_dir
		else:
			self.tmpdir = tempfile.TemporaryDirectory()
			self.root = self.tmpdir.name

	def load_repo(self, repo_dir='', repo_url=None):

		if not repo_dir and repo_url:
			repo_dir = repo_url.replace("https://github.com/", '').replace("/", '-')

		repo_fullpath = self.root + "/" + repo_dir

		if os.path.exists(repo_fullpath) and repo_funcs.is_git_dir(repo_fullpath):
			self.repo = Repo(repo_fullpath)
			return True

		if repo_url:
			self.repo = Repo.clone_from(repo_url, repo_fullpath)
			return True

		raise Exception("Invalid repo directory & repo url supplied")



c1_xor_c2 = "{c1}...{c2}"
c2_and_not_c1 = "{c1}..{c2}"
c1_parents = "{c1}^@"
c1_not_nth_parent = "{c1}^{n}"
c1_not_self = "{c1}^"


class CommitStats:

	def __init__(self, c):
		self.csha = c.hexsha
		self.author = c.author.email
		self.authored_date = c.authored_datetime
		self.committer = c.committer.email
		self.committed_date = c.committed_datetime.replace(tzinfo=None)
		self.commit_msg = c.message
		self.file_stats = c.stats.files
		self.overall_stats = c.stats.total


class GitData(object):

	def __init__(self, repo):
		self.repo = repo

	def get_commit_file_diff(self, c1, c2):
		c1 = self.repo.commit(c1)
		c2 = self.repo.commit(c2)
		return c1.diff(c2)

	def get_commit_stats(self, c):

		c = self.repo.commit(c)
		return CommitStats(c)

	def get_commits_stats(self, range_c):

		stats = []

		commits = self.repo.git.rev_list(range_c)
		if not commits:
			return stats

		commit_list = commits.split()

		for c in commit_list:
			commit = Commit(self.repo, hex_to_bin(c))
			stats.append(CommitStats(commit))

		return stats

	def get_commit_stats_range(self, c1, c2):

		range_c = c1_xor_c2.format(c1=c1, c2=c2)
		return self.get_commits_stats(range_c)

	def get_merge_commit_stats(self, mc):

		mc_master = c1_not_nth_parent.format(c1=mc, n=1)
		mc_branch = c1_not_nth_parent.format(c1=mc, n=2)

		try:
			split_commit = self.repo.git.merge_base(mc_master, mc_branch)
		except Exception as e:
			return [], [], None

		range_branch = c2_and_not_c1.format(c1=mc_master, c2=mc_branch)

		dev_stats = self.get_commits_stats(range_branch)

		range_master = c2_and_not_c1.format(c1=mc_branch, c2=mc_master)

		master_stats = self.get_commits_stats(range_master)

		return dev_stats, master_stats, split_commit
