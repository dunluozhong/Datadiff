oracle 12c以上 sqlserver 2016以上 

数据库账号能访问系统表

# 1. 配置python环境及安装相关驱动表
   
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

chmod +x Miniconda3-latest-Linux-x86_64.sh

./Miniconda3-latest-Linux-x86_64.sh

source ~/.bashrc

conda create -n Datadiff 'python=3.12' -y    

conda activate Datadiff

yum install unixODBC unixODBC-devel freetds freetds-devel -y

pip install flask pysqlite3 data-diff psycopg2-binary mysql-connector-python pyodbc oracledb Flask-Session

安装sqlserver驱动包(非必需)

wget https://packages.microsoft.com/rhel/7/prod/msodbcsql18-18.1.2.1-1.x86_64.rpm

ACCEPT_EULA=Y yum localinstall -y msodbcsql18-18.1.2.1-1.x86_64.rpm 

# 2. 启动

git clone https://github.com/dunluozhong/Datadiff

cd Datadiff

python app.py

