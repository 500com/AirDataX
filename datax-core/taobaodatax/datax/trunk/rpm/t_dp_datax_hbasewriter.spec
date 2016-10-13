summary: DataX HBaseWriter can write data to HBase
Name:t_dp_datax_hbasewriter
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
mkdir -p %{dataxpath}/plugins/writer/hbasewriter

cp ${OLDPWD}/../src/com/taobao/datax/plugins/writer/hbasewriter/ParamKey.java %{dataxpath}/plugins/writer/hbasewriter
cp ${OLDPWD}/../build/plugins/hbasewriter-1.0.0.jar %{dataxpath}/plugins/writer/hbasewriter
cp ${OLDPWD}/../build/plugins/plugins-common-1.0.0.jar %{dataxpath}/plugins/writer/hbasewriter
cp -r ${OLDPWD}/../libs/hadoop-0.20.jar %{dataxpath}/plugins/writer/hbasewriter
cp -r ${OLDPWD}/../libs/zookeeper-3.3.3.jar %{dataxpath}/plugins/writer/hbasewriter
cp -r ${OLDPWD}/../libs/commons-logging-1.1.1.jar %{dataxpath}/plugins/writer/hbasewriter
cp -r ${OLDPWD}/../libs/hbase-0.90.2.jar %{dataxpath}/plugins/writer/hbasewriter

%files
%defattr(0755,root,root)
%{dataxpath}/plugins/writer/hbasewriter

%changelog
* Thu Oct 25 2011 zhuzhuang 
- Version 1.0.0
- svn tag address
- http://svn.simba.taobao.com/svn/DW/arch/trunk/cheetah/services/datax/tools/dataexchange
