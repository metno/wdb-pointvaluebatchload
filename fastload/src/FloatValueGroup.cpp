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

#include "FloatValueGroup.h"
#include <boost/date_time/posix_time/posix_time.hpp>

namespace fastload
{
namespace
{
FloatValueGroup::Duration getDuration(const std::string & durationText)
{
	return boost::posix_time::duration_from_string(durationText);
}
}

FloatValueGroup::FloatValueGroup(
			long long dataproviderid,
			long long placeid,
			Duration validtimefrom,
			Duration validtimeto,
			int validtimeindeterminatecode,
			int valueparameterid,
			int levelparameterid,
			float levelfrom,
			float levelto,
			int levelindeterminatecode,
			int dataversion) :
		dataproviderid_(dataproviderid),
		placeid_(placeid),
		validtimefrom_(validtimefrom),
		validtimeto_(validtimeto),
		validtimeindeterminatecode_(validtimeindeterminatecode),
		valueparameterid_(valueparameterid),
		levelparameterid_(levelparameterid),
		levelfrom_(levelfrom),
		levelto_(levelto),
		levelindeterminatecode_(levelindeterminatecode),
		dataversion_(dataversion)
{}


FloatValueGroup::FloatValueGroup(const pqxx::result::tuple & queryResult) :
		dataproviderid_(queryResult[1].as<long long>()),
		placeid_(queryResult[2].as<long long>()),
		validtimefrom_(getDuration(queryResult[3].as<std::string>())),
		validtimeto_(getDuration(queryResult[4].as<std::string>())),
		validtimeindeterminatecode_(queryResult[5].as<int>()),
		valueparameterid_(queryResult[6].as<int>()),
		levelparameterid_(queryResult[7].as<int>()),
		levelfrom_(queryResult[8].as<float>()),
		levelto_(queryResult[9].as<float>()),
		levelindeterminatecode_(queryResult[10].as<int>()),
		dataversion_(queryResult[11].as<int>())
{
}

FloatValueGroup::~FloatValueGroup()
{
}

namespace
{
std::string format(const FloatValueGroup::Duration & duration)
{
	std::ostringstream s;
	s << duration.hours() << ':' << duration.minutes() << ':' << duration.seconds();
	return s.str();
}
}

std::string FloatValueGroup::databaseInsertStatement(int valueGroupId) const
{
	std::ostringstream query;
	query << "INSERT INTO wdb_int.floatvaluegroup VALUES (" <<
			valueGroupId <<','<<
			dataproviderid_ <<','<<
			placeid_ <<','<<
			'\'' << format(validtimefrom_) << "',"<<
			'\'' << format(validtimeto_) << "',"<<
			0 <<','<<
			valueparameterid_ <<','<<
			levelparameterid_ <<','<<
			levelfrom_ <<','<<
			levelto_ <<','<<
			0 <<','<<
			dataversion_ << ")";

	return query.str();
}

bool operator < (const FloatValueGroup & a, const FloatValueGroup & b)
{
	if (a.dataproviderid() != b.dataproviderid())
		return a.dataproviderid() < b.dataproviderid();
	if (a.placeid() != b.placeid())
		return a.placeid() < b.placeid();
	if (a.validtimefrom() != b.validtimefrom())
		return a.validtimefrom() < b.validtimefrom();
	if (a.validtimeto() != b.validtimeto())
		return a.validtimeto() < b.validtimeto();
	if (a.validtimeindeterminatecode() != b.validtimeindeterminatecode())
		return a.validtimeindeterminatecode() < b.validtimeindeterminatecode();
	if (a.valueparameterid() != b.valueparameterid())
		return a.valueparameterid() < b.valueparameterid();
	if (a.levelparameterid() != b.levelparameterid())
		return a.levelparameterid() < b.levelparameterid();
	if (a.levelfrom() != b.levelfrom())
		return a.levelfrom() < b.levelfrom();
	if (a.levelto() != b.levelto())
		return a.levelto() < b.levelto();
	if (a.levelindeterminatecode() != b.levelindeterminatecode())
		return a.levelindeterminatecode() < b.levelindeterminatecode();
	return a.dataversion() < b.dataversion();
}

} /* namespace fastload */
