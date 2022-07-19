#Get file from git
import os
import git
import shutil
import tempfile

t = tempfile.mkdtemp()

git.Repo.clone_from('git@github.com:nickloadd/scripts.git', t, branch='main', depth=1)

shutil.move(os.path.join(t, 'LogRotation.py'), '.')

shutil.rmtree(t)