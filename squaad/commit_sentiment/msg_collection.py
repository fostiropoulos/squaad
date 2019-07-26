from squaad.db_lite import BaseConverters
from squaad import db_lite
from dataclasses import dataclass
from squaad.git_local import GitRepo, GitData
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import WordPunctTokenizer


@dataclass
class CommitMessage(BaseConverters):
    app: str
    csha: str
    raw: str
    preprocessed: str


def preprocess(raw):
    tokenized = WordPunctTokenizer().tokenize(raw.lower())
    res = ' '.join([WordNetLemmatizer().lemmatize(x) for x in tokenized])
    return res


def populate_table():
    db = db_lite.db("../../config.json")
    db.add_table(CommitMessage)

    apps = db.table_to_pd('applications', cols=['repo_url', 'application'], index_col='application')
    commits = db.table_to_pd('commits', cols=['application', 'csha'])
    commits_by_app = commits.groupby(by='application')

    for app, app_commits in commits_by_app:
        repo_url = apps.loc[app].repo_url
        git_repo = GitRepo()
        git_repo.load_repo(repo_url=repo_url)
        git_data = GitData(git_repo.repo)

        for commit in app_commits.csha:
            raw = git_data.get_commit_stats(commit).commit_msg
            db.add_row(CommitMessage(app,
                                   commit,
                                   raw,
                                   preprocess(raw)))

if __name__ == '__main__':
    populate_table()
