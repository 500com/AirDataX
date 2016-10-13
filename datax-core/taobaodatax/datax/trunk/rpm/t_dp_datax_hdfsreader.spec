summary: DataX hdfsreader can read data from hadoop-hdfs 
Name: t_dp_datax_hdfsreader
Version: 1.0.0
Release: 1
Group: System
License: GPL
BuildArch: noarch
AutoReqProv: no 
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX hdfsreader can read data from hadoop-hdfs 

%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/reader/hdfsreader

cp ${OLDPWD}/../src/com/taobao/datax/plugins/reader/hdfsreader/ParamKey.java %{dataxpath}/plugins/reader/hdfsreader/
cp ${OLDPWD}/../build/plugins/hdfsreader-1.0.0.jar %{dataxpath}/plugins/reader/hdfsreader/
cp ${OLDPWD}/../build/plugins/plugins-common-1.0.0.jar %{dataxpath}/plugins/reader/hdfsreader
cp -r ${OLDPWD}/../libs/hadoop-0.19.2-core.jar %{dataxpath}/plugins/reader/hdfsreader
cp -r ${OLDPWD}/../libs/commons-logging-1.1.1.jar %{dataxpath}/plugins/reader/hdfsreader
cp -r ${OLDPWD}/../libs/libhadoop.so %{dataxpath}/plugins/reader/hdfsreader

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/reader/hdfsreader

%changelog
* Fri Aug 20 2010 meining 
- Version 1.0.0

