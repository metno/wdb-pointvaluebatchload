#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT(wdb-fastload, 0.5, [wdb@met.no])

AC_CONFIG_SRCDIR([src/CopyJob.cpp])
AC_CONFIG_MACRO_DIR([m4])
AC_PROG_RANLIB

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_MAKE_SET
AC_PROG_LN_S


# Checks for libraries.

WDB_BOOST_BASE_HEADERS(1.40.0)
WDB_BOOST_PROGRAM_OPTIONS(1.40.0)
WDB_BOOST_THREAD(1.40.0)
WDB_LOG4CPP_CHECK(1.0)
WDB_PQXX_CHECK(2.6.8)



# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE

AM_INIT_AUTOMAKE([foreign -Wall -Werror subdir-objects])
AM_PROG_CC_C_O

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
