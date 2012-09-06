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

#include "DatabaseTranslator.h"
#include <log4cpp/Category.hh>
#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <algorithm>

namespace fastload
{

DatabaseTranslator::DatabaseTranslator(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace) :
		connection_(pqConnectString), transaction_(0), wciUser_(wciUser), nameSpace_(nameSpace)
{
	connection_.set_variable("timezone", "UTC");
}

DatabaseTranslator::~DatabaseTranslator()
{
	commit();
}

std::string DatabaseTranslator::updateDataprovider(const std::string & dataproviderSpec)
{
	std::vector<std::string> elements;
	boost::split(elements, dataproviderSpec, boost::is_any_of("\t"));

	if ( elements.size() > 2 or elements.empty() )
		throw std::runtime_error("Invalid spec for dataprovider: " + dataproviderSpec);

	const std::string dataprovider = elements[0];

	if ( elements.size() == 2 or not nameSpace_.empty() )
	{
		std::string ns = nameSpace_.empty() ? elements[1] : nameSpace_;
		exec("SELECT wci.begin('" + transaction().esc(wciUser_) + "', " + transaction().esc(ns) + ")");
	}
	else // if ( elements.size() == 1 )
		exec("SELECT wci.begin('" + transaction().esc(wciUser_) + "')");

	return dataprovider;
}

long long DatabaseTranslator::dataproviderid(const std::string & dataprovidername)
{
	long long & dataproviderid = dataproviders_[dataprovidername];
	if ( ! dataproviderid )
	{
		std::ostringstream query;
		query << "SELECT wci_int.idfromdataprovider('" << transaction().esc(dataprovidername) << "')";
		pqxx::result result = exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized dataprovider: " + dataprovidername);
		dataproviderid = result[0][0].as<long long>();
	}
	return dataproviderid;
}

namespace
{
	inline Duration duration(long long seconds)
	{
		return Duration(seconds / 3600, 0, seconds % 3600);
	}
	std::string timeQuery(const std::string & rowName)
	{
		return "to_char(" + rowName + ", 'YYYY-MM-DD HH24:MI:SS')";
	}
	Time parseTime(const pqxx::result::field & field, const Time & defaultTime)
	{
		if ( field.is_null() )
			return defaultTime;
		return boost::posix_time::time_from_string(field.as<std::string>());
	}
}
long long DatabaseTranslator::placeid(const std::string & placename, const Time & time)
{
	std::map<Time, long long> & placeid = placeids_[placename];
	if ( placeid.empty() )
	{
		std::ostringstream query;
		query << "SELECT pv.placeid, " << timeQuery("pv.placenamevalidfrom") << ", " << timeQuery("pv.placenamevalidto") << " FROM wci_int.placedefinition_mv pv, wci_int.getsessiondata() s WHERE pv.placename='" << transaction().esc(placename) << "' AND pv.placenamespaceid = s.placenamespaceid";
		//query << "SELECT placeid, " << timeQuery("placenamevalidfrom") << ", " << timeQuery("placenamevalidto") << " FROM wci.getplacedefinition('" << transaction().esc(placename) << "') ORDER BY placenamevalidfrom";

		pqxx::result result = exec(query.str());
		//if ( result.empty() )
		//	throw std::runtime_error("Not a recognized placename: " + placename);

		//placeid[negativeInfinity] = -1;
		for ( pqxx::result::const_iterator it = result.begin(); it != result.end(); ++ it )
		{
			static const Time epoch(Date(1970, 1, 1));
			Time from = parseTime((*it)[1], negativeInfinity);
			Time to = parseTime((*it)[2], infinity);

			placeid[from] = (*it)[0].as<long long>();
			placeid[to] = -1;
		}
	}
	//std::map<Time, long long>::const_iterator find = placeid.lower_bound(time);
	std::map<Time, long long>::const_reverse_iterator find = placeid.rbegin();
	while ( find != placeid.rend() )
	{
		if ( find->first <= time)
			break;
		++ find;
	}

	if ( find == placeid.rend() or find->second < 0 )
		throw std::runtime_error("Unable to find a valid point for placename <" + placename + "> at time " + to_simple_string(time));

	return find->second;



//	long long & placeid = placeids_[placename];
//	if ( ! placeid )
//	{
//		std::ostringstream query;
//		query << "SELECT placeid, placenamevalidfrom, placenamevalidto FROM wci.getplacedefinition('" << transaction().esc(placename) << "')";
//		pqxx::result result = exec(query.str());
//		if ( result.empty() )
//			throw std::runtime_error("Not a recognized placename: " + placename);
//		placeid = result[0][0].as<long long>();
//	}
//	return placeid;
}

int DatabaseTranslator::valueparameterid(const std::string & parametername)
{
	int & paramid = valueparameterids_[parametername];
	if ( ! paramid )
	{
		std::ostringstream query;
		query << "SELECT wci_int.getvalueparameterid(wci_int.normalizeparameter('" << transaction().esc(parametername) << "'))";
		pqxx::result result = exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized value parameter: " + parametername);
		paramid = result[0][0].as<int>();
	}
	return paramid;
}

int DatabaseTranslator::levelparameterid(const std::string & parametername)
{
	int & paramid = levelparameterids_[parametername];
	if ( ! paramid )
	{
		std::ostringstream query;
		query << "SELECT wci_int.getlevelparameterid(wci_int.normalizelevelparameter('" << transaction().esc(parametername) << "'))";
		pqxx::result result = exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized level parameter: " + parametername);
		paramid = result[0][0].as<int>();
	}
	return paramid;
}

std::string DatabaseTranslator::now()
{
	if ( timeNow_.empty() )
	{
		pqxx::result result = exec("SELECT now()");
		timeNow_ = result[0][0].as<std::string>();
	}
	return timeNow_;
}

int DatabaseTranslator::getValueGroup(const std::string & dataprovidername,
		const std::string & placename,
		const Time & referenceTime,
		const Duration & validTimeFrom,
		const Duration & validTimeTo,
		const std::string & valueparametername,
		const std::string & levelparametername,
		float levelfrom,
		float levelto,
		int dataversion)
{
	long long dataproviderId = dataproviderid(dataprovidername);
	long long placeId = placeid(placename, referenceTime);
	std::string validfrom = transaction().esc(to_simple_string(validTimeFrom));
	std::string validto = transaction().esc(to_simple_string(validTimeTo));
	int valueparameterId = valueparameterid(valueparametername);
	int levelparameterId = levelparameterid(levelparametername);

	FloatValueGroup wantedValueGroup(dataproviderId, placeId, validTimeFrom, validTimeTo, 0, valueparameterId, levelparameterId, levelfrom, levelto, 0, dataversion);

	std::map<FloatValueGroup, int>::const_iterator find = floatValueGroups_.find(wantedValueGroup);
	if ( find == floatValueGroups_.end() ) // was not in memory, so we must contact database
	{
		if ( queriedDataprovidersAndPlaces_.find(std::make_pair(dataproviderId, placeId)) == queriedDataprovidersAndPlaces_.end() )
		{
			queriedDataprovidersAndPlaces_.insert(std::make_pair(dataproviderId, placeId));
			std::stringstream query;
			query << "SELECT * FROM wdb_int.floatvaluegroup WHERE "
					"dataproviderid=" << dataproviderId << " AND "
					"placeid=" << placeId;
			pqxx::result result = exec(query.str());

			for ( pqxx::result::const_iterator it = result.begin(); it != result.end(); ++ it )
				floatValueGroups_[*it] = (*it)[0].as<int>();
			find = floatValueGroups_.find(wantedValueGroup);
		}

		if ( find == floatValueGroups_.end() ) // was not in database, so we must create storage
		{
			pqxx::result id = exec("SELECT nextval('wdb_int.floatvaluegroup_valuegroupid_seq')");
			int valueGroupId = id.at(0).at(0).as<int>();

			std::string insert = wantedValueGroup.databaseInsertStatement(valueGroupId);
			exec(insert);
			floatValueGroups_[wantedValueGroup] = valueGroupId;
			return valueGroupId;
		}
	}
	return find->second;
}

std::string DatabaseTranslator::getDataTableName(const Time & referenceTime)
{
	if ( destinationTables_.empty() )
	{
		pqxx::result result = exec("SELECT schemaname, tablename "
				"FROM pg_catalog.pg_tables "
				"WHERE schemaname='wdb_partition' AND tablename='floatvalueitem_partitions'");
		if ( result.empty() )
		{
			TablePartition partition("", fastload::parseTime("-infinity"), fastload::parseTime("infinity"));
			destinationTables_.push_back(partition);
		}
		else
		{
			result = exec("SELECT * FROM wdb_partition.floatvalueitem_partitions ORDER BY fromtime");
			for ( pqxx::result::const_iterator it = result.begin(); it != result.end(); ++ it )
			{
				std::string tableName = (*it)[0].as<std::string>();
				Time validFrom = fastload::parseTime((*it)[1].as<std::string>());
				Time validTo = fastload::parseTime((*it)[2].as<std::string>());
				TablePartition partition(tableName, validFrom, validTo);
				destinationTables_.push_back(partition);
			}
		}
	}

	for ( std::vector<TablePartition>::const_reverse_iterator find = destinationTables_.rbegin(); find != destinationTables_.rend(); ++ find )
		if ( find->validFrom <= referenceTime and referenceTime < find->validTo )
			return find->tableName;

	throw std::runtime_error("No partition available for time " + boost::posix_time::to_simple_string(referenceTime));
}

std::string DatabaseTranslator::wciVersion()
{
	if ( wciVersion_.empty() )
	{
		pqxx::result r = exec("SELECT wci.version()");
		wciVersion_ = r[0][0].as<std::string>();
	}
	return wciVersion_;
}

void DatabaseTranslator::commit()
{
	if ( transaction_ )
	{
		transaction_->commit();
		delete transaction_;
		transaction_ = 0;
	}
}

pqxx::work & DatabaseTranslator::transaction()
{
	if ( ! transaction_ )
		transaction_ = new pqxx::work(connection_);
	return * transaction_;
}

pqxx::result DatabaseTranslator::exec(const std::string & query)
{
	log4cpp::Category & log = log4cpp::Category::getInstance("wdb.query");
	log.debug(query);

	return transaction().exec(query);
}

} /* namespace fastload */
