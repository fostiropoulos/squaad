from squaad.db_lite import BaseConverters
from squaad import db_lite
from dataclasses import dataclass
from datetime import datetime
from squaad.git_local import GitRepo, GitData


@dataclass
class MergeCommit(BaseConverters):
	id_: int
	app: str
	split_commit: str
	merge_commit: str
	date_split: datetime
	date_merged: datetime
	files_changed: str
	loc_added: int
	loc_removed: int
	loc_total: int


@dataclass
class MasterCommit(BaseConverters):
	id_: int
	csha: str
	date: datetime
	author: str
	files_changed: str
	loc_added: int
	loc_removed: int
	loc_total: int


@dataclass
class BranchCommit(BaseConverters):
	id_: int
	csha: str
	date: datetime
	author: str
	files_changed: str
	loc_added: int
	loc_removed: int
	loc_total: int

def populate_merge_tables():
	db = db_lite.db("../../config.json")
	db.add_table(MergeCommit)
	db.add_table(MasterCommit)
	db.add_table(BranchCommit)

	apps = db.table_to_pd('applications', cols=['repo_url', 'application'], index_col='application')
	commits = db.table_to_pd('cpairs', cols=['application', 'prev', 'curr'])
	commits_by_app = commits.groupby(by='application')

	id_ = 0
	for app, app_commits in commits_by_app:
		repo_url = apps.loc[app].repo_url
		git_repo = GitRepo()
		git_repo.load_repo(repo_url=repo_url)
		git_data = GitData(git_repo.repo)

		merge_commits = app_commits[app_commits.duplicated(subset='curr')].curr
		for merge_commit in merge_commits:
			merge_stats, dev_stats, master_stats, split_commit, split_date = git_data.get_merge_commit_stats(merge_commit)

			if split_commit:
				db.add_row(MergeCommit(id_,
									   app,
									   split_commit,
									   merge_commit,
									   split_date,
									   merge_stats.committed_date,
									   " ".join(merge_stats.file_stats.keys()),
									   merge_stats.overall_stats['insertions'],
									   merge_stats.overall_stats['deletions'],
									   merge_stats.overall_stats['lines']))

				for c in dev_stats:
					bc = BranchCommit(id_,
										c.csha,
										c.committed_date,
										c.committer,
										" ".join(c.file_stats.keys()),
										c.overall_stats['insertions'],
										c.overall_stats['deletions'],
										c.overall_stats['lines'])
					db.add_row(bc)

				for c in master_stats:
					mc = MasterCommit(id_,
										c.csha,
										c.committed_date,
										c.committer,
										" ".join(c.file_stats.keys()),
										c.overall_stats['insertions'],
										c.overall_stats['deletions'],
										c.overall_stats['lines'])
					db.add_row(mc)

				id_ += 1


if __name__ == '__main__':
    populate_merge_tables()