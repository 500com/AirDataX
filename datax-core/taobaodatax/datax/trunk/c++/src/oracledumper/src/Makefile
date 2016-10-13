INCLUDE=-I../include
LIBS=-lclntsh -liconv -L../lib -L${ORACLE_HOME}/lib -L../../../../libs/
CC=g++
OBJS=liboraclewriter.so
CFLAGS=-shared -fPIC -Wl,-rpath=/home/taobao/datax/libs
CPP=common.cpp dumper.cpp oradumper.cpp strsplit.cpp com_taobao_datax_plugins_writer_oraclewriter_OracleWriterJni.cpp

OBJS: $(CPP)
	$(CC) $(INCLUDE) -o $(OBJS) $(CPP) $(CFLAGS) $(LIBS)
clean:
	rm -rf $(OBJS)
