oracle 12c以上 sqlserver 2016以上

1. 配置python环境及安装相关驱动表
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
source ~/.bashrc
conda create -n data-diff 'python=3.12' -y     
conda activate data-diff
pip install flask pysqlite3 data-diff psycopg2-binary mysql-connector-python pyodbc oracledb Flask-Session -i https://mirrors.aliyun.com/pypi/simple


2. 启动
git clone https://github.com/dunluozhong/Datadiff
python app.py

