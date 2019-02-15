from squaad import db_lite
from dataclasses import dataclass
from datetime import datetime
from squaad.git_local import GitRepo, GitData
import pandas as pd
from functools import reduce


def collect_commit_data(name):

	df_apps = pd.read_csv(name, usecols=["Project Name", "Commit ID"])
	df_apps = df_apps.dropna()
	db = db_lite.db("../../config.json")

	apps = db.table_to_pd('applications', cols=['repo_url', 'application'], index_col='application')

	commits_by_app = df_apps.groupby(by="Project Name")

	stats = []
	for app, app_commits in commits_by_app:
		print(app)
		app_match = "-".join(list("\w" + x[1:].lower() for x in app.strip().split()))
		repo = apps[apps.index.str.contains(app_match)]
		if repo.size != 1:
			print("regex error")
			continue
		repo_url = repo.iloc[0,0]
		git_repo = GitRepo()
		git_repo.load_repo(repo_url=repo_url)
		git_data = GitData(git_repo.repo)

		for commit in app_commits["Commit ID"]:
			commit_data = git_data.get_commit_stats(commit.strip())
			stats.append([app, commit, commit_data.commit_msg,
							commit_data.overall_stats['insertions'],
					   		commit_data.overall_stats['deletions'],
					   		commit_data.overall_stats['lines'],
					   		commit_data.overall_stats['files'],
					   		", ".join(list(commit_data.file_stats.keys()))])

	df = pd.DataFrame(stats, columns=['app', 'commit', 'msg', 'insertions', 'deletions', 'lines', 'files', 'file_names'])

	df.to_csv(name.split(".")[0] + "_new.csv")
	print("data saved")

if __name__ == '__main__':
	for name in ["/Users/alex.polak/Downloads/Consistency Between Commit Message and Content - Breaker.csv",
				 "/Users/alex.polak/Downloads/Consistency Between Commit Message and Content - Neutral.csv"]:
		collect_commit_data(name)
