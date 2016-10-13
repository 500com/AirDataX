summary: DataX mysqlreader can read data from mysql
Name: t_dp_datax_mysqlreader
Version: 1.0.0
Release: 1
Group: System
License: GPL
AutoReqProv: no 
BuildArch: noarch
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX mysqlreader can read data from mysql


%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/reader/mysqlreader

cp ${OLDPWD}/../src/com/taobao/datax/plugins/reader/mysqlreader/ParamKey.java %{dataxpath}/plugins/reader/mysqlreader
cp ${OLDPWD}/../build/plugins/mysqlreader-1.0.0.jar %{dataxpath}/plugins/reader/mysqlreader
cp ${OLDPWD}/../build/plugins/plugins-common-1.0.0.jar %{dataxpath}/plugins/reader/mysqlreader
cp -r ${OLDPWD}/../libs/mysql-connector-java-5.1.18-bin.jar %{dataxpath}/plugins/reader/mysqlreader
cp -r ${OLDPWD}/../libs/commons-dbcp-1.4.jar %{dataxpath}/plugins/reader/mysqlreader
cp -r ${OLDPWD}/../libs/commons-pool-1.5.4.jar %{dataxpath}/plugins/reader/mysqlreader
cp -r ${OLDPWD}/../libs/commons-logging-1.1.1.jar %{dataxpath}/plugins/reader/mysqlreader

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/reader/mysqlreader

%changelog
* Fri Aug 20 2010 meining 
- Version 1.0.0

