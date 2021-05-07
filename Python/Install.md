# Instructions to install Spark 

## To install Spark in MAC and LINUX
Thanks to [lukaskawerau post](https://www.lukaskawerau.com/local-pyspark-jupyter-mac/)

1. Install JAVA JDK.
2. GO to Apache Spark Download Page [here.](https://spark.apache.org/downloads.html) or by direct [link.](spark-3.0.2-bin-hadoop3.2.tgz)
3. Go to folder with spark are.
4. Extract using :
```bash
tar -xzf spark-3.0.2-bin-hadoop3.2.tgz
```
5. Move folder of spark to opt folder
```bash
sudo mv spark-3.0.2-bin-hadoop3.2 /opt/spark-3.0.2
```
6. Create a symbolic link (symlink) to your Spark version
```bash
sudo ln -s /opt/spark-3.0.2 /opt/spark
```
What’s happening here? By creating a symbolic link to our specific version (3.0.1) we can have multiple versions installed in parallel and only need to adjust the symlink to work with them.
7. Tell your shell where to find Spark
Until macOS 10.14 the default shell used in the Terminal app was bash, but from 10.15 on it is Zshell (zsh). So depending on your version of macOS, you need to do one of the following:

```bash
nano ~/.bashrc # for macOs(10.14)
nano ~/.zshrc # for macOs(10.15)
```
Set Spark variables in your ~/.bashrc/~/.zshrc file
```bash
# Spark
export SPARK_HOME="/opt/spark"
export PATH=$SPARK_HOME/bin:$PATH
export SPARK_LOCAL_IP='127.0.0.1'
```
### Installing Pyspark in venv
In your terminal 
1. make a python venv
```bash
python3 -m venv spark_env
```
2. Enter and activate environment
```bash
source ./spark_env/bin/activate
```
3. Install via PIP Jupyter and Pyspark
```bash
pip3 install pyspark
pip3 install jupyter
python3 -m ipykernel install --user
```
4. Now tell Pyspark to use Jupyter: in your ~/.bashrc/~/.zshrc file, add
```bash
# pyspark
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
```
Your ~/.bashrc or ~/.zshrc should now have a section that looks kinda like this:

```bash
# Spark
export SPARK_HOME="/opt/spark"
export PATH=$SPARK_HOME/bin:$PATH
export SPARK_LOCAL_IP='127.0.0.1'

# Pyspark
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS=notebook
```
Now you save the file, and source your Terminal:
```bash
source ~/.bashrc 
# or
source ~/.zshrc
```
To start Pyspark and open up Jupyter, you can simply run $ pyspark. You only need to make sure you’re inside your pipenv environment. That means:

Go to your pyspark folder ($ cd ~/<your folder>)
Type ```pyspark```

## To install Spark in Windows

Follow this steps on this [post.](https://www.datacamp.com/community/tutorials/installation-of-pyspark) 
Obs.: Do not maintain your spark folder at downloads folder.

## Other references about Environment
* https://towardsdatascience.com/how-to-successfully-install-anaconda-on-a-mac-and-actually-get-it-to-work-53ce18025f97
* https://www.jcchouinard.com/how-to-use-anaconda-environments/
* https://stackoverflow.com/questions/60416082/pyspark-numpy-not-found-in-cluster-mode-modulenotfounderror
* https://medium.com/swlh/pyspark-on-macos-installation-and-use-31f84ca61400
* https://medium.com/@alexandrecastro_20139/de-noob-para-noob-configurando-meu-primeiro-cluster-com-hadoop-spark-pyspark-e-jupyter-notebook-e61062fe3450
* https://qastack.com.br/programming/30518362/how-do-i-set-the-drivers-python-version-in-spark
* https://medium.com/@achilleus/get-started-with-pyspark-on-mac-using-an-ide-pycharm-b8cbad7d516f
