ACLOCAL_AMFLAGS = -I m4

noinst_LIBRARIES = libpointvaluebatchload.a
libpointvaluebatchload_a_SOURCES =
libpointvaluebatchload_a_CPPFLAGS = $(wdb_pointvaluebatchload_CPPFLAGS)


bin_PROGRAMS = pointValueBatchLoad

pointValueBatchLoad_SOURCES =

pointValueBatchLoad_CPPFLAGS = $(wdb_CFLAGS)
pointValueBatchLoad_LDADD = libpointvaluebatchload.a $(wdb_LIBS) $(BOOST_THREAD_LIB) $(BOOST_PROGRAM_OPTIONS_LIB) $(BOOST_DATE_TIME_LIB) -lpq


man1_MANS = pointValueBatchLoad.man

pointValueBatchLoad.man: pointValueBatchLoad
	help2man -N -n"Fast bulk loading of data into a wdb database" ./$< > $@


include src/fastload.mk
include test/fastload.mk

EXTRA_DIST = example/example.dat
CLEANFILES = $(man1_MANS)

include mk/debian.mk