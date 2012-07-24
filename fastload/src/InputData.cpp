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

#include "InputData.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
#include <stdexcept>

namespace fastload
{
namespace
{
boost::posix_time::ptime timeFromString(std::string s)
{
	if ( s.size() < 19 )
		throw std::invalid_argument("invalid time specification");
	s[10] = ' '; // In case 'T' was used as a separator
	s.resize(19);
	return boost::posix_time::time_from_string(s);
}
}

template<typename T>
T cast(const std::string & s)
{
	try
	{
		return boost::lexical_cast<T>(s);
	}
	catch ( boost::bad_lexical_cast & )
	{
		throw std::runtime_error(s + " could not be interpreted as " + std::string(typeid(T).name()));
	}
}

InputData::InputData(const std::string & inputLine, const std::string dataprovider) :
		dataprovider_(dataprovider)
{
	std::vector<std::string> splitData;
	boost::split(splitData, inputLine, boost::is_any_of("\t"));

	if ( splitData.size() < 9 or splitData.size() > 11 )
		throw std::runtime_error("Error in input data: " + inputLine);

	while ( splitData.size() < 11 )
		splitData.push_back("0");

	value_ = cast<float>(splitData[0]);
	placename_ = splitData[1];
	referencetime_ = timeFromString(splitData[2]);
	validfrom_ = timeFromString(splitData[3]);
	validto_ = timeFromString(splitData[4]);
	valueparametername_ = splitData[5];
	levelparametername_ = splitData[6];
	levelfrom_ = cast<float>(splitData[7]);
	levelto_ = cast<float>(splitData[8]);
	dataversion_ = cast<int>(splitData[9]);
	maxdataversion_ = cast<int>(splitData[10]);
}

InputData::~InputData()
{
}

}
