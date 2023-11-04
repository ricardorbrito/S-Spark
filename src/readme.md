### package manager for [macos]
```sh
# root
/Users/luanmorenomaciel

# package manager
https://brew.sh/
https://docs.brew.sh/Manpage

# install
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# update
brew update
```

### install apache-spark
```shell
# info
brew info apache-spark

# install spark
brew install apache-spark
brew reinstall apache-spark

# force [conflict] fix
brew link --overwrite apache-spark

# different version?
brew search apache-spark
brew install eddies/spark-tap/apache-spark@2.4.6

# loc
/usr/local/Cellar/

# test
spark-shell
http://localhost:4040/jobs/
```

### install [pip] using [brew]
```shell
python3 -m pip install --upgrade setuptools
python3 -m pip install --upgrade pip
```

### install [pyspark]
```shell
# root
/Users/luanmorenomaciel

# verify python
python3 --version

# install & create env
# project [location]
pip install virtualenv
/Users/luanmorenomaciel/GitHub/series-spark
virtualenv venv

# activate
source venv/bin/activate

# pip install
pip install -r requirements.txt

# verify version
pyspark --version
```