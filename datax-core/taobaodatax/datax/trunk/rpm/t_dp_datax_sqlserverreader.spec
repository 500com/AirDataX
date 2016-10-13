summary: DataX sqlserverreader can read data from sqlserver
Name: t_dp_datax_sqlserverreader
Version: 1.0.0
Release: 1
Group: System
License: GPL
BuildArch: noarch
AutoReqProv: no
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX sqlserverreader can read data from sqlserver


%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/reader/sqlserverreader

cp ${OLDPWD}/../src/com/taobao/datax/plugins/reader/sqlserverreader/ParamKey.java %{dataxpath}/plugins/reader/sqlserverreader
cp ${OLDPWD}/../build/plugins/sqlserverreader-1.0.0.jar %{dataxpath}/plugins/reader/sqlserverreader
cp ${OLDPWD}/../build/plugins/plugins-common-1.0.0.jar %{dataxpath}/plugins/reader/sqlserverreader

cp -r ${OLDPWD}/../libs/sqljdbc4.jar %{dataxpath}/plugins/reader/sqlserverreader
cp -r ${OLDPWD}/../libs/commons-dbcp-1.4.jar %{dataxpath}/plugins/reader/sqlserverreader
cp -r ${OLDPWD}/../libs/commons-pool-1.5.4.jar %{dataxpath}/plugins/reader/sqlserverreader
cp -r ${OLDPWD}/../libs/commons-logging-1.1.1.jar %{dataxpath}/plugins/reader/sqlserverreader

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/reader/sqlserverreader

%changelog
* Fri Aug 20 2010 zhouxiaolong
- Version 1.0.0

