summary: DataX oraclewriter can write data to oracle 
Name: t_dp_datax_oraclewriter
Version: 1.0.0
Release: 1
Group: System
License: GPL
AutoReqProv: no
BuildArch: noarch
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX oraclewriter can write data to oracle 

%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/writer/oraclewriter

cp ${OLDPWD}/../src/com/taobao/datax/plugins/writer/oraclewriter/ParamKey.java %{dataxpath}/plugins/writer/oraclewriter
cp ${OLDPWD}/../c++/build/liboraclewriter.so %{dataxpath}/plugins/writer/oraclewriter
cp ${OLDPWD}/../build/plugins/oraclewriter-1.0.0.jar %{dataxpath}/plugins/writer/oraclewriter
cp ${OLDPWD}/../build/plugins/plugins-common-1.0.0.jar %{dataxpath}/plugins/writer/oraclewriter

cp -r ${OLDPWD}/../libs/libiconv.so.2 %{dataxpath}/plugins/writer/oraclewriter
cp -r ${OLDPWD}/../libs/libcharset.so %{dataxpath}/plugins/writer/oraclewriter

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/writer/oraclewriter

%changelog
* Fri Aug 20 2010 meining 
- Version 1.0.0

