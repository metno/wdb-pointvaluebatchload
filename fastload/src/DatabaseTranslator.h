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

#ifndef DATABASETRANSLATOR_H_
#define DATABASETRANSLATOR_H_

#include "FloatValueGroup.h"
#include <pqxx/connection.hxx>
#include <pqxx/transaction>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <string>
#include <map>
#include <set>


namespace fastload
{

class DatabaseTranslator
{
public:
	DatabaseTranslator(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace);
	~DatabaseTranslator();

	std::string updateDataprovider(const std::string & dataproviderSpec);

	long long dataproviderid(const std::string & dataprovidername);
	long long placeid(const std::string & placename);
	int valueparameterid(const std::string & parametername);
	int levelparameterid(const std::string & parametername);
	std::string now();
	typedef boost::posix_time::time_duration Duration;

	int getValueGroup(const std::string & dataprovidername,
			const std::string & placename,
			const Duration & validTimeFrom,
			const Duration & validTimeTo,
			const std::string & valueparametername,
			const std::string & levelparametername,
			float levelfrom,
			float levelto,
			int dataversion);

private:
	pqxx::result exec(const std::string & query);

	pqxx::connection connection_;
	pqxx::work * transaction_;
	std::string wciUser_;
	std::string nameSpace_;

	std::map<std::string, long long> dataproviders_;
	std::map<std::string, long long> placeids_;
	std::map<std::string, int> valueparameterids_;
	std::map<std::string, int> levelparameterids_;
	std::string timeNow_;

	std::map<FloatValueGroup, int> floatValueGroups_;
	std::set<std::pair<long long, long long> > queriedDataprovidersAndPlaces_;
};

} /* namespace fastload */
#endif /* DATABASETRANSLATOR_H_ */
