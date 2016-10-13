summary: DataX FakeReader for test
Name:t_dp_datax_fakereader
Version: 1.0.0
Release: 1
Group: System
License: GPL
BuildArch: noarch
AutoReqProv: no 
Requires: t_dp_datax_engine

%define dataxpath /home/taobao/datax

%description
datax

%prep
cd ${OLDPWD}/../
export LANG=zh_CN.UTF-8
ant dist

%build

%install
mkdir -p %{dataxpath}/plugins/reader/fakereader

cp ${OLDPWD}/../src/com/taobao/datax/plugins/reader/fakereader/ParamKey.java %{dataxpath}/plugins/reader/fakereader
cp ${OLDPWD}/../build/plugins/fakereader-1.0.0.jar %{dataxpath}/plugins/reader/fakereader

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/reader/fakereader

%changelog
* Fri Mar 25 2011 bazhen.csy
- Version 1.0.0
- svn tag address
- http://svn.simba.taobao.com/svn/DW/arch/trunk/cheetah/services/datax/tools/dataexchange/
