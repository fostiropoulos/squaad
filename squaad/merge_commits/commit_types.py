import datetime
from dataclasses import dataclass, field
from typing import *
import sqlalchemy


def sql(obj):
	return obj.__sql__()


def sql_table(obj):
	return obj.__sql_table__()


@dataclass
class CodeChange:
	file_name: str
	total_changed: int
	total_added: int
	total_deleted: int


@dataclass
class GitCommit:
	sha: str

	def __sql__(self):
		return ((self.sha), )

	@classmethod
	def __sql_table__(cls):
		return (("sha", sqlalchemy.Text), )


@dataclass
class BranchCommit(GitCommit):
	master_or_dev: str
	merge_commit: GitCommit
	split_commit: GitCommit
	time: datetime
	author: str
	files: List[CodeChange]  # list of file names and number of lines changed
	total_changed: int = field(init=False)
	total_added: int = field(init=False)

	def __post_init__(self):
		self.total_changed = sum(x.total_changed for x in self.files)
		self.total_added = sum(x.total_added for x in self.files)

	def __sql__(self):
		return ((self.merge_commit.sha, self.split_commit.sha, self.master_or_dev, self.sha, self.time,\
			   self.author, self.total_changed, self.total_added), )

	@classmethod
	def __sql_table__(cls):
		return (("merge_commit", sqlalchemy.Text), ("split_commit", sqlalchemy.Text), ("master_or_dev", sqlalchemy.Text),
				("commit", sqlalchemy.Text), ("date", sqlalchemy.Time), ("author", sqlalchemy.Text), ("changed", sqlalchemy.Integer), ("added", sqlalchemy.Integer))


@dataclass
class MergeCommit(GitCommit):
	parent_master: str
	parent_branch: str


@dataclass
class Branch:
	master_or_dev: str
	merge_commit: GitCommit
	split_commit: GitCommit
	commits: List[BranchCommit]
	num_commits: int = field(init=False)
	first_commit: BranchCommit = field(init=False)
	last_commit: BranchCommit = field(init=False)

	def __post_init__(self):
		self.first_commit = self.commits[0] if self.commits else None
		self.last_commit = self.commits[-1] if self.commits else None
		self.num_commits = len(self.commits)

	def __sql__(self):
		return ((self.merge_commit.sha, self.split_commit.sha, self.master_or_dev, \
			   self.num_commits, self.first_commit.sha, self.last_commit.sha), )

	@classmethod
	def __sql_table__(cls):
		return (("merge_commit", sqlalchemy.Text), ("split_commit", sqlalchemy.Text), ("master_or_dev", sqlalchemy.Text),
				("num_commits", sqlalchemy.Integer), ("first_commit", sqlalchemy.Text), ("last_commit", sqlalchemy.Text))

@dataclass
class BranchMergeGroup:
	app: str
	merge_commit: GitCommit
	split_commit: GitCommit
	master_branch: Branch
	dev_branch: Branch

	def __sql__(self):
		return ((self.app, self.split_commit.sha, self.merge_commit.sha), )

	@classmethod
	def __sql_table__(cls):
		return (("application", sqlalchemy.Text), ("split_commit", sqlalchemy.Text), ("merge_commit",
																					   sqlalchemy.Text))