summary: DataX oraclereader can read data from oracle
Name: t_dp_datax_oraclereader
Version: 1.0.0
Release: 1
Group: System
License: GPL
BuildArch: noarch
AutoReqProv: no
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX oraclereader can read data from oracle

%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/reader/oraclereader

cp ${OLDPWD}/../src/com/taobao/datax/plugins/reader/oraclereader/ParamKey.java %{dataxpath}/plugins/reader/oraclereader
cp ${OLDPWD}/../build/plugins/oraclereader-1.0.0.jar %{dataxpath}/plugins/reader/oraclereader
cp ${OLDPWD}/../build/plugins/plugins-common-1.0.0.jar %{dataxpath}/plugins/reader/oraclereader
cp -r ${OLDPWD}/../libs/ojdbc14_10.2.0.4.jar %{dataxpath}/plugins/reader/oraclereader
cp -r ${OLDPWD}/../libs/commons-dbcp-1.4.jar %{dataxpath}/plugins/reader/oraclereader
cp -r ${OLDPWD}/../libs/commons-pool-1.5.4.jar %{dataxpath}/plugins/reader/oraclereader
cp -r ${OLDPWD}/../libs/commons-logging-1.1.1.jar %{dataxpath}/plugins/reader/oraclereader

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/reader/oraclereader

%changelog
* Fri Aug 20 2010 meining 
- Version 1.0.0

