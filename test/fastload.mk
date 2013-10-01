TESTS = pointValueBatchLoadTest

check_PROGRAMS = pointValueBatchLoadTest

pointValueBatchLoadTest_SOURCES = \
	test/InputDataTest.cpp \
	test/timetypetest.cpp
 

pointValueBatchLoadTest_CPPFLAGS = \
	$(AM_CPPFLAGS) \
	$(pointValueBatchLoad_CPPFLAGS) \
	-I$(top_srcdir)/src \
	$(BOOST_CPPFLAGS)

pointValueBatchLoadTest_LDADD = \
	$(pointValueBatchLoad_LDADD) \
	$(BOOST_UNIT_TEST_FRAMEWORK_LIB)
