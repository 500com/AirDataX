summary: DataX httpreader can read data from http
Name: t_dp_datax_httpreader
Version: 1.0.0
Release: 1
Group: System
License: GPL
BuildArch: noarch
AutoReqProv: no 
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX httpreader can read data from http


%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/reader/httpreader

cp ${OLDPWD}/../src/com/taobao/datax/plugins/reader/httpreader/ParamKey.java %{dataxpath}/plugins/reader/httpreader
cp ${OLDPWD}/../build/plugins/httpreader-1.0.0.jar %{dataxpath}/plugins/reader/httpreader

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/reader/httpreader

%changelog
* Fri Aug 12 2011 hejianchao.pt
- Version 1.0.0
