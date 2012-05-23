ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS = wdb-fastload

wdb_fastload_SOURCES = \
	src/main.cpp \
	src/AbstractDatabaseJob.cpp \
	src/AbstractDatabaseJob.h \
	src/AbstractJob.cpp \
	src/AbstractJob.h \
	src/Configuration.cpp \
	src/Configuration.h \
	src/CopyJob.cpp \
	src/CopyJob.h \
	src/DataQue.cpp \
	src/DataQue.h \
	src/InputData.cpp \
	src/InputData.h \
	src/TranslateJob.cpp \
	src/TranslateJob.h

wdb_fastload_CPPFLAGS = $(wdb_CFLAGS)
wdb_fastload_LDADD = $(wdb_LIBS) $(BOOST_THREAD_LIB) $(BOOST_PROGRAM_OPTIONS_LIB) -lpq

man1_MANS = wdb-fastload.man

wdb-fastload.man:	wdb-fastload
	help2man -N -n"Fast bulk loading of data into a wdb database" ./$< > $@

EXTRA_DIST = example/example.dat

include mk/debian.mk