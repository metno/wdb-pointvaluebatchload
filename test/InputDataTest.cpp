/*
 fastload 

 Copyright (C) 2012 met.no

 Contact information:
 Norwegian Meteorological Institute
 Box 43 Blindern
 0313 OSLO
 NORWAY
 E-mail: post@met.no

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 MA  02110-1301, USA
 */

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE FastloadTest
#include <boost/test/unit_test.hpp>

#include <InputData.h>


BOOST_AUTO_TEST_CASE( test )
{
	fastload::InputData d("100\t1005\t1999-04-15T18:00:00+00\t1999-04-17T03:00:00+00\t1999-04-17T04:00:00+00\tcloud area fraction\tatmosphere sigma coordinate\t1000\t1001", "someprovider");

	BOOST_CHECK_EQUAL("someprovider", d.dataprovider());
	BOOST_CHECK_EQUAL(100, d.value());
	BOOST_CHECK_EQUAL("1005", d.placename());
	BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("1999-04-15 18:00:00"), d.referencetime());
	BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("1999-04-17 03:00:00"), d.validfrom());
	BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("1999-04-17 04:00:00"), d.validto());
	BOOST_CHECK_EQUAL("cloud area fraction", d.valueparametername());
	BOOST_CHECK_EQUAL("atmosphere sigma coordinate", d.levelparametername());
	BOOST_CHECK_EQUAL(1000, d.levelfrom());
	BOOST_CHECK_EQUAL(1001, d.levelto());
	BOOST_CHECK_EQUAL(0, d.dataversion());
	BOOST_CHECK_EQUAL(0, d.maxdataversion());
}

BOOST_AUTO_TEST_CASE( timestampsWithTimeZones )
{
	fastload::InputData d("100\t1005\t1999-04-15T18:00:00+02\t1999-04-17T03:00:00+00\t1999-04-17T04:00:00+00\tcloud area fraction\tatmosphere sigma coordinate\t1000\t1001", "someprovider");

	BOOST_CHECK_EQUAL(boost::posix_time::time_from_string("1999-04-15 20:00:00"), d.referencetime());
}

BOOST_AUTO_TEST_CASE( throwOnMissingEntry )
{
	BOOST_CHECK_THROW(
			fastload::InputData("100\t1005\t1999-04-15T18:00:00+02\t1999-04-17T03:00:00+00\t1999-04-17T04:00:00+00\tcloud area fraction\tatmosphere sigma coordinate\t1000", "someprovider"),
			std::runtime_error
			);
}

BOOST_AUTO_TEST_CASE( throwOnWrongValueFormat )
{
	BOOST_CHECK_THROW(
			fastload::InputData("NOT_A_VALUE\t1005\t1999-04-15T18:00:00+02\t1999-04-17T03:00:00+00\t1999-04-17T04:00:00+00\tcloud area fraction\tatmosphere sigma coordinate\t1000\t1001", "someprovider"),
			std::runtime_error
			);
}


