from commit_types import *

import psycopg2 as psycopg2
import tempfile
from git import Repo, Commit
import sqlalchemy


def connect_db(db_name, user):
	conn = psycopg2.connect(host="localhost", database=db_name, user=user)
	return conn


def connect_sqlalchemy_db(db_name, user):
	connection_string = "postgresql+psycopg2://{}@localhost:5432/{}".format(user, db_name)
	db = sqlalchemy.create_engine(connection_string)
	engine = db.connect()
	meta = sqlalchemy.MetaData(engine)
	return engine, meta


class GitReader(object):
	class TempGitRepo(NamedTuple):
		repo: Repo
		dir: tempfile.TemporaryDirectory

	def __init__(self):
		self.git_repo = None  # type: GitReader.TempGitRepo

	def add_application(self, repo_url: str):

		directory = tempfile.TemporaryDirectory()
		repo = Repo.clone_from(repo_url, directory.name)  # type: Repo
		self.git_repo = GitReader.TempGitRepo(repo, directory)

	def remove_application(self):
		self.git_repo.dir.cleanup()

	def get_start_commit(self, mc: MergeCommit):

		res = self.git_repo.repo.merge_base(mc.parent_master, mc.parent_branch)
		if not res:
			return None
		first_base = res[0]
		commit = GitCommit(first_base.hexsha)
		return commit

	def to_branch_commit(self, bc: Commit, master_or_dev: str, mc, sc):
		return BranchCommit(bc.hexsha,
							master_or_dev,
							mc,
							sc,
							bc.committed_datetime,
							bc.author.email,
							[CodeChange(k, v['lines'], v['insertions'], v['deletions']) for k, v in bc.stats.files.items()])

	def get_branches_from_merge(self, mc: MergeCommit, sc: GitCommit, name: str):

		branch_commits = []
		master_commits = []

		commits_master_span = mc.parent_master + "..." + sc.sha
		commits_branch_span = mc.parent_branch + "..." + sc.sha

		commit_iter = self.git_repo.repo.iter_commits(commits_branch_span)
		for c in commit_iter:
			branch_commits.append(self.to_branch_commit(c, "D", mc, sc))

		commit_iter = self.git_repo.repo.iter_commits(commits_master_span)
		for c in commit_iter:
			master_commits.append(self.to_branch_commit(c, "M", mc, sc))

		return BranchMergeGroup(name,
								mc,
								sc,
								Branch("M", mc, sc, list(reversed(master_commits))),
								Branch("D", mc, sc, list(reversed(branch_commits))))



class SqlDb(object):

	def __init__(self, db_name: str, user: str):
		self.conn = connect_db(db_name, user)  # type: psycopg2._ext.connection
		self.apps_to_tables = {}
		self.engine, self.meta = connect_sqlalchemy_db(db_name, user)


	def get_next_app(self):
		""" read from applications table and yield all"""
		cur = self.conn.cursor()
		cur.execute("select * from applications;")
		for repo, name in cur:
			yield repo, name

	def get_next_merge_commit(self, app):
		cur = self.conn.cursor()
		cur.execute("select application, prev, curr from cpairs where application = %s and curr in "
					"(select curr from cpairs group by curr ""having count(curr) ""= 2);", (app, ))

		while True:
			first = cur.fetchone()
			if not first:
				return
			second = cur.fetchone()
			bp = first[1]
			merge = first[2]
			mp = second[1]
			yield MergeCommit(merge, mp, bp)

	def create_table(self, name: str, class_):

		col_defs = sql_table(class_)
		columns = (sqlalchemy.Column(*x) for x in col_defs)

		t = sqlalchemy.Table(name, self.meta, *columns)
		self.meta.create_all()

		self.apps_to_tables[name] = t

	def add_row(self, table: str, obj):
		stmnt = self.apps_to_tables[table].insert().values(sql(obj))
		self.engine.execute(stmnt)


if __name__ == '__main__':

	db = SqlDb("esem_db", "alex.polak")
	db.create_table("merge_groups", BranchMergeGroup)
	db.create_table("merge_group_branches", Branch)
	db.create_table("merge_group_branch_commit", BranchCommit)

	g = GitReader()

	for repo, name in db.get_next_app():
		g.add_application(repo)
		for mc in db.get_next_merge_commit(name):
			sc = g.get_start_commit(mc)
			if sc:
				merge_group = g.get_branches_from_merge(mc, sc, name)
				db.add_row("merge_groups", merge_group)
				b1 = merge_group.master_branch
				b2 = merge_group.dev_branch
				db.add_row("merge_group_branches", b1)
				db.add_row("merge_group_branches", b2)
				for c in b1.commits:
					db.add_row("merge_group_branch_commit", c)
				for c in b2.commits:
					db.add_row("merge_group_branch_commit", c)

		g.remove_application()


