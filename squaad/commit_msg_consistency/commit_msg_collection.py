from squaad import db_lite
from squaad.git_local import GitRepo, GitData
import pandas as pd


def collect_commit_data(name):

	df_apps = pd.read_csv(name, usecols=["Project Name", "Commit ID"])
	df_apps = df_apps.dropna()
	db = db_lite.db("../../config.json")

	apps = db.table_to_pd('applications', cols=['repo_url', 'application'], index_col='application')

	commits_by_app = df_apps.groupby(by="Project Name")

	stats = []
	for app, app_commits in commits_by_app:
		print(app)
		repo_url = apps.loc[app, 'repo_url']
		git_repo = GitRepo()
		git_repo.load_repo(repo_url=repo_url)
		git_data = GitData(git_repo.repo)

		for commit in app_commits["Commit ID"]:
			commit_data = git_data.get_commit_stats(commit.strip())

			cstats = [app,
					  commit,
					  commit_data.commit_msg,
					  commit_data.overall_stats.added,
					  commit_data.overall_stats.removed,
					  commit_data.overall_stats.changed,
					  len(commit_data.overall_stats.files),
					  sum([len(x.chunks) for x in commit_data.overall_stats.files.values()])]
			cstats.extend(commit_data.entropy_lines_across_files())
			cstats.append(commit_data.entropy_chunks_across_files())
			cstats.extend(commit_data.entropy_lines_across_chunks())

			stats.append(cstats)

	df = pd.DataFrame(stats, columns=['app',
									  'commit',
									  'msg',
									  'added',
									  'removed',
									  'changed',
									  'n_files',
									  'n_chunks',
									  'entropy_lines_added',
									  'entropy_lines_removed',
									  'entropy_lines_changed',
									  'entropy_chunks',
									  'entropy_lines_chunks_added',
									  'entropy_lines_chunks_removed',
									  'entropy_lines_chunks_changed'])

	df.to_csv(name + "_new.csv")
	print("data saved")

if __name__ == '__main__':
	for name in ["/Users/alex.polak/Downloads/Consistency Between Commit Message and Content - Breaker (1).csv",
				 "/Users/alex.polak/Downloads/Consistency Between Commit Message and Content - Neutral.csv"]:
		collect_commit_data(name)
