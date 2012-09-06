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



#include "timetypes.h"
#include <string>

namespace fastload
{
const Time negativeInfinity(boost::posix_time::neg_infin);

const Time infinity(boost::posix_time::pos_infin);

namespace
{
Duration parseTimeZone(const std::string & timezone)
{
	// Z is the only time zone name that is accepted
	if ( timezone == "Z" or timezone == "z" )
		return Duration(0,0,0);
	try
	{
		return boost::posix_time::hours(boost::lexical_cast<int>(timezone));
	}
	catch ( boost::bad_lexical_cast &)
	{
		throw std::logic_error("Unable to parse <" + timezone + "> as a time zone");
	}
}
}

Time parseTime(std::string s)
{
	if ( s == "-infinity" )
		return Time(boost::posix_time::neg_infin);
	if ( s == "infinity")
		return Time(boost::posix_time::pos_infin);

	if ( s.size() < 19 )
		throw std::invalid_argument("invalid time specification");
	s[10] = ' '; // In case 'T' was used as a separator

	std::string timezone = "Z";
	if ( s.size() > 19 )
		timezone = s.substr(19);

	s.resize(19);
	return boost::posix_time::time_from_string(s) + parseTimeZone(timezone);
}

}
