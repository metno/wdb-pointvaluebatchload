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


#ifndef TIMETYPES_H_
#define TIMETYPES_H_

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>


namespace fastload
{

typedef boost::gregorian::date Date;
typedef boost::posix_time::ptime Time;
typedef boost::posix_time::time_duration Duration;

extern const Time infinity;
extern const Time negativeInfinity;

/// Parse a time string, as required in input files
Time parseTime(std::string time);


}




#endif /* TIMETYPES_H_ */
