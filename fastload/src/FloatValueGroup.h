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

#ifndef FLOATVALUEGROUP_H_
#define FLOATVALUEGROUP_H_

#include <pqxx/result>
#include <boost/flyweight.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>

namespace fastload
{
namespace internal
{
/**
 * FloatValueGroup data that is expected to vary very little over a single
 * loading session. Allows the use of a flyweight pattern to reduce memory
 * usage for huge data sets.
 */
struct ImmutableData
{
	ImmutableData(
			long long dataproviderid,
			long long placeid,
			int validtimeindeterminatecode,
			int valueparameterid,
			int levelparameterid,
			float levelfrom,
			float levelto,
			int levelindeterminatecode
	);

	long long dataproviderid_;
	long long placeid_;
	int validtimeindeterminatecode_;
	int valueparameterid_;
	int levelparameterid_;
	float levelfrom_;
	float levelto_;
	int levelindeterminatecode_;
};
}


/**
 * In-memory representation of a floatvaluegroup entry in a wdb database.
 */
class FloatValueGroup
{
public:
    typedef boost::posix_time::time_duration Duration;

    FloatValueGroup(
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
			int dataversion
	);
	FloatValueGroup(const pqxx::result::tuple & queryResult);
	~FloatValueGroup();

	/**
	 * Get the SQL statement to insert *this into a wdb database.
	 *
	 * @param valudeGroupId the value to use for valuegroupid in the database
	 */
	std::string databaseInsertStatement(int valueGroupId) const;

	/**
	 * Get string formatted versions of all elements in *this, in the order
	 * expected by fastload's floatvalueitem COPY job.
	 *
	 * @param valudeGroupId the value to use for valuegroupid in the database
	 * @returns a list of strings, with one element for each column in the
	 *          database.
	 */
	std::vector<std::string> elements(int valueGroupId) const;

    long long dataproviderid() const
    {
        return immutable_.get().dataproviderid_;
    }

    long long placeid() const
    {
        return immutable_.get().placeid_;
    }

    const Duration & validtimefrom() const
    {
        return validtimefrom_;
    }

    const Duration & validtimeto() const
    {
        return validtimeto_;
    }

    int validtimeindeterminatecode() const
    {
        return immutable_.get().validtimeindeterminatecode_;
    }

    int valueparameterid() const
    {
        return immutable_.get().valueparameterid_;
    }

    int levelparameterid() const
    {
        return immutable_.get().levelparameterid_;
    }

    float levelfrom() const
    {
        return immutable_.get().levelfrom_;
    }

    float levelto() const
    {
        return immutable_.get().levelto_;
    }

    int levelindeterminatecode() const
    {
        return immutable_.get().levelindeterminatecode_;
    }

    int dataversion() const
    {
        return dataversion_;
    }


private:

    boost::flyweight<internal::ImmutableData> immutable_;
	Duration validtimefrom_;
	Duration validtimeto_;
	int dataversion_;
};

bool operator < (const FloatValueGroup & a, const FloatValueGroup & b);

namespace internal
{
bool operator == (const ImmutableData & a, const ImmutableData & b);
std::size_t hash_value(const ImmutableData & id);
}

} /* namespace fastload */
#endif /* FLOATVALUEGROUP_H_ */
