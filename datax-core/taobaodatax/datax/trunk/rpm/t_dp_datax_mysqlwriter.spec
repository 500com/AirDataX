summary: DataX mysql can write data to mysql
Name: t_dp_datax_mysqlwriter
Version: 1.0.0
Release: 1
Group: System
License: GPL
BuildArch: noarch
AutoReqProv: no
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX mysql can write data to mysql

%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/writer/mysqlwriter

cp ${OLDPWD}/../src/com/taobao/datax/plugins/writer/mysqlwriter/ParamKey.java %{dataxpath}/plugins/writer/mysqlwriter
cp ${OLDPWD}/../build/plugins/mysqlwriter-1.0.0.jar %{dataxpath}/plugins/writer/mysqlwriter
cp ${OLDPWD}/../build/plugins/plugins-common-1.0.0.jar %{dataxpath}/plugins/writer/mysqlwriter

cp -r ${OLDPWD}/../libs/mysql-connector-java-5.1.18-bin.jar %{dataxpath}/plugins/writer/mysqlwriter
cp -r ${OLDPWD}/../libs/commons-dbcp-1.4.jar %{dataxpath}/plugins/writer/mysqlwriter
cp -r ${OLDPWD}/../libs/commons-pool-1.5.4.jar %{dataxpath}/plugins/writer/mysqlwriter
cp -r ${OLDPWD}/../libs/commons-logging-1.1.1.jar %{dataxpath}/plugins/writer/mysqlwriter

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/writer/mysqlwriter

%changelog
* Fri Aug 20 2010 meining 
- Version 1.0.0


