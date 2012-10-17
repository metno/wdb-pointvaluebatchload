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

#include <boost/test/unit_test.hpp>

#include <timetypes.h>

using namespace fastload;

namespace
{
Time time(const std::string & t)
{
	return boost::posix_time::time_from_string(t);
}
}

BOOST_AUTO_TEST_CASE( parseTimeUtc )
{
	BOOST_CHECK_EQUAL(time("1999-04-15 18:00:00"), parseTime("1999-04-15 18:00:00Z"));
	BOOST_CHECK_EQUAL(time("1999-04-15 18:00:00"), parseTime("1999-04-15 18:00:00+00"));
}

BOOST_AUTO_TEST_CASE( parseTimeWithTimeZone )
{
	BOOST_CHECK_EQUAL(time("1999-04-15 20:00:00"), parseTime("1999-04-15 18:00:00+02"));
}
