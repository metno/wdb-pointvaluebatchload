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
#include <boost/date_time/posix_time/posix_time_duration.hpp>

namespace fastload
{

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

	std::string databaseInsertStatement(int valueGroupId) const;

    long long dataproviderid() const
    {
        return dataproviderid_;
    }

    long long placeid() const
    {
        return placeid_;
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
        return validtimeindeterminatecode_;
    }

    int valueparameterid() const
    {
        return valueparameterid_;
    }

    int levelparameterid() const
    {
        return levelparameterid_;
    }

    float levelfrom() const
    {
        return levelfrom_;
    }

    float levelto() const
    {
        return levelto_;
    }

    int levelindeterminatecode() const
    {
        return levelindeterminatecode_;
    }

    int dataversion() const
    {
        return dataversion_;
    }


private:
	 long long dataproviderid_;
	 long long placeid_;
	 Duration validtimefrom_;
	 Duration validtimeto_;
	 int validtimeindeterminatecode_;
	 int valueparameterid_;
	 int levelparameterid_;
	 float levelfrom_;
	 float levelto_;
	 int levelindeterminatecode_;
	 int dataversion_;
};

bool operator < (const FloatValueGroup & a, const FloatValueGroup & b);

} /* namespace fastload */
#endif /* FLOATVALUEGROUP_H_ */
