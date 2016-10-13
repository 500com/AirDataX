summary: DataX streamreader can read data from stream
Name: t_dp_datax_streamreader
Version: 1.0.0
Release: 1
Group: System
License: GPL
BuildArch: noarch
AutoReqProv: no
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
DataX streamreader can read data from stream


%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/reader/streamreader

cp ${OLDPWD}/../src/com/taobao/datax/plugins/reader/streamreader/ParamKey.java %{dataxpath}/plugins/reader/streamreader
cp ${OLDPWD}/../build/plugins/streamreader-1.0.0.jar %{dataxpath}/plugins/reader/streamreader

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/reader/streamreader

%changelog
* Fri Mar 25 2011 bazhen.csy
- Version 1.0.0
