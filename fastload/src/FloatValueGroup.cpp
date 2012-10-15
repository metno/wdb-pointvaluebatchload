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
		immutable_(internal::ImmutableData(dataproviderid, placeid, validtimeindeterminatecode, valueparameterid, levelparameterid, levelfrom, levelto, levelindeterminatecode)),
		validtimefrom_(validtimefrom),
		validtimeto_(validtimeto),
		dataversion_(dataversion)
{}


FloatValueGroup::FloatValueGroup(const pqxx::result::tuple & queryResult) :
		immutable_(internal::ImmutableData(
				queryResult[1].as<long long>(),
				queryResult[2].as<long long>(),
				queryResult[5].as<int>(),
				queryResult[6].as<int>(),
				queryResult[7].as<int>(),
				queryResult[8].as<float>(),
				queryResult[9].as<float>(),
				queryResult[10].as<int>())),
		validtimefrom_(getDuration(queryResult[3].as<std::string>())),
		validtimeto_(getDuration(queryResult[4].as<std::string>())),
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
			dataproviderid() <<','<<
			placeid() <<','<<
			'\'' << format(validtimefrom_) << "',"<<
			'\'' << format(validtimeto_) << "',"<<
			0 <<','<<
			valueparameterid() <<','<<
			levelparameterid() <<','<<
			levelfrom() <<','<<
			levelto() <<','<<
			0 <<','<<
			dataversion_ << ")";

	return query.str();
}

namespace
{
template<typename T>
std::string str(T t)
{
	return boost::lexical_cast<std::string>(t);
}
}

std::vector<std::string> FloatValueGroup::elements(int valueGroupId) const
{
	std::vector<std::string> ret;
	ret.push_back(str(valueGroupId));
	ret.push_back(str(dataproviderid()));
	ret.push_back(str(placeid()));
	ret.push_back(format(validtimefrom_));
	ret.push_back(format(validtimeto_));
	ret.push_back("0");
	ret.push_back(str(valueparameterid()));
	ret.push_back(str(levelparameterid()));
	ret.push_back(str(levelfrom()));
	ret.push_back(str(levelto()));
	ret.push_back("0");
	ret.push_back(str(dataversion()));
	return ret;
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

namespace internal
{
ImmutableData::ImmutableData(
		long long dataproviderid,
		long long placeid,
		int validtimeindeterminatecode,
		int valueparameterid,
		int levelparameterid,
		float levelfrom,
		float levelto,
		int levelindeterminatecode) :
				dataproviderid_(dataproviderid),
				placeid_(placeid),
				validtimeindeterminatecode_(validtimeindeterminatecode),
				valueparameterid_(valueparameterid),
				levelparameterid_(levelparameterid),
				levelfrom_(levelfrom),
				levelto_(levelto),
				levelindeterminatecode_(levelindeterminatecode)
{}

bool operator == (const ImmutableData & a, const ImmutableData & b)
{
	return a.dataproviderid_ == b.dataproviderid_
		and a.placeid_ == b.placeid_
		and a.validtimeindeterminatecode_ == b.validtimeindeterminatecode_
		and a.valueparameterid_ == b.valueparameterid_
		and a.levelparameterid_ == b.levelparameterid_
		and a.levelfrom_ == b.levelfrom_
		and a.levelto_ == b.levelto_
		and a.levelindeterminatecode_ == b.levelindeterminatecode_;
}

std::size_t hash_value(const ImmutableData & id)
{
	std::size_t seed = 0;

	boost::hash_combine(seed, id.dataproviderid_);
	boost::hash_combine(seed, id.placeid_);
	boost::hash_combine(seed, id.validtimeindeterminatecode_);
	boost::hash_combine(seed, id.valueparameterid_);
	boost::hash_combine(seed, id.levelparameterid_);
	boost::hash_combine(seed, id.levelfrom_);
	boost::hash_combine(seed, id.levelto_);
	boost::hash_combine(seed, id.levelindeterminatecode_);

	return seed;
}

}

} /* namespace fastload */
