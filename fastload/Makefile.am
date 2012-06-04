ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS = wdb-fastload

wdb_fastload_SOURCES =

wdb_fastload_CPPFLAGS = -I$(top_srcdir)/src/ $(wdb_CFLAGS)
wdb_fastload_LDADD = $(wdb_LIBS) $(BOOST_THREAD_LIB) $(BOOST_PROGRAM_OPTIONS_LIB) -lpq

man1_MANS = wdb-fastload.man

wdb-fastload.man:	wdb-fastload
	help2man -N -n"Fast bulk loading of data into a wdb database" ./$< > $@


include src/fastload.mk

EXTRA_DIST = example/example.dat

include mk/debian.mk