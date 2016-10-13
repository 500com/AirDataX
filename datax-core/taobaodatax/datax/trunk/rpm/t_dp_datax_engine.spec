summary: engine provides core scheduler and data swap storage for DataX 
Name: t_dp_datax_engine
Version: 1.0.0
Release: 1
Group: System
License: GPL
AutoReqProv: no 
BuildArch: noarch

%define dataxpath  /home/taobao/datax

%description
DataX Engine provides core scheduler and data swap storage for DataX 

%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
dos2unix ${OLDPWD}/../release/datax.py

mkdir -p %{dataxpath}/bin
mkdir -p %{dataxpath}/conf
mkdir -p %{dataxpath}/engine
mkdir -p %{dataxpath}/common
mkdir -p %{dataxpath}/libs
mkdir -p %{dataxpath}/jobs
mkdir -p %{dataxpath}/logs

cp ${OLDPWD}/../jobs/sample/*.xml %{dataxpath}/jobs
cp ${OLDPWD}/../release/*.py %{dataxpath}/bin/

cp -r ${OLDPWD}/../conf/*.properties %{dataxpath}/conf
cp -r ${OLDPWD}/../conf/*.xml %{dataxpath}/conf

cp -r ${OLDPWD}/../build/engine/*.jar %{dataxpath}/engine

cp -r ${OLDPWD}/../build/common/*.jar %{dataxpath}/common
cp ${OLDPWD}/../c++/build/libcommon.so %{dataxpath}/common

cp -r ${OLDPWD}/../libs/commons-io-2.0.1.jar %{dataxpath}/libs
cp -r ${OLDPWD}/../libs/commons-lang-2.4.jar %{dataxpath}/libs
cp -r ${OLDPWD}/../libs/dom4j-2.0.0-ALPHA-2.jar %{dataxpath}/libs
cp -r ${OLDPWD}/../libs/jaxen-1.1-beta-6.jar %{dataxpath}/libs
cp -r ${OLDPWD}/../libs/junit-4.4.jar %{dataxpath}/libs
cp -r ${OLDPWD}/../libs/log4j-1.2.16.jar %{dataxpath}/libs
cp -r ${OLDPWD}/../libs/slf4j-api-1.4.3.jar %{dataxpath}/libs
cp -r ${OLDPWD}/../libs/slf4j-log4j12-1.4.3.jar %{dataxpath}/libs

%post
chmod -R 0777 %{dataxpath}/jobs
chmod -R 0777 %{dataxpath}/logs

%files
%defattr(0755,root,root)
%{dataxpath}/bin
%{dataxpath}/conf
%{dataxpath}/engine
%{dataxpath}/common
%{dataxpath}/libs
%attr(0777,root,root) %dir %{dataxpath}/logs
%attr(0777,root,root) %dir %{dataxpath}/jobs


%changelog
* Fri Aug 20 2010 meining 
- Version 1.0.0

