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
#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace fastload
{

DatabaseTranslator::DatabaseTranslator(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace) :
		connection_(pqConnectString), wciUser_(wciUser), nameSpace_(nameSpace)
{
	transaction_ = new pqxx::work(connection_);
}

DatabaseTranslator::~DatabaseTranslator()
{
	transaction_->commit();
	delete transaction_;
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
		transaction_->exec("SELECT wci.begin('" + transaction_->esc(wciUser_) + "', " + transaction_->esc(ns) + ")");
	}
	else // if ( elements.size() == 1 )
		transaction_->exec("SELECT wci.begin('" + transaction_->esc(wciUser_) + "')");

	return dataprovider;
}

long long DatabaseTranslator::dataproviderid(const std::string & dataprovidername)
{
	long long & dataproviderid = dataproviders_[dataprovidername];
	if ( ! dataproviderid )
	{
		std::ostringstream query;
		query << "SELECT wci_int.idfromdataprovider('" << transaction_->esc(dataprovidername) << "')";
		pqxx::result result = transaction_->exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized dataprovider: " + dataprovidername);
		dataproviderid = result[0][0].as<long long>();
	}
	return dataproviderid;
}

long long DatabaseTranslator::placeid(const std::string & placename)
{
	long long & placeid = placeids_[placename];
	if ( ! placeid )
	{
		std::ostringstream query;
		query << "SELECT placeid FROM wci.getplacedefinition('" << transaction_->esc(placename) << "')";
		pqxx::result result = transaction_->exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized placename: " + placename);
		placeid = result[0][0].as<long long>();
	}
	return placeid;
}

int DatabaseTranslator::valueparameterid(const std::string & parametername)
{
	int & paramid = valueparameterids_[parametername];
	if ( ! paramid )
	{
		std::ostringstream query;
		query << "SELECT wci_int.getvalueparameterid(wci_int.normalizeparameter('" << transaction_->esc(parametername) << "'))";
		pqxx::result result = transaction_->exec(query.str());
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
		query << "SELECT wci_int.getlevelparameterid(wci_int.normalizelevelparameter('" << transaction_->esc(parametername) << "'))";
		pqxx::result result = transaction_->exec(query.str());
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
		pqxx::result result = transaction_->exec("SELECT now()");
		timeNow_ = result[0][0].as<std::string>();
	}
	return timeNow_;
}

int DatabaseTranslator::getValueGroup(const std::string & dataprovidername,
		const std::string & placename,
		const Duration & validTimeFrom,
		const Duration & validTimeTo,
		const std::string & valueparametername,
		const std::string & levelparametername,
		float levelfrom,
		float levelto,
		int dataversion)
{
	long long dataproviderId = dataproviderid(dataprovidername);
	long long placeId = placeid(placename);
	std::string validfrom = transaction_->esc(to_simple_string(validTimeFrom));
	std::string validto = transaction_->esc(to_simple_string(validTimeTo));
	int valueparameterId = valueparameterid(valueparametername);
	int levelparameterId = levelparameterid(levelparametername);

	std::stringstream query;
	query << "SELECT valuegroupid FROM wdb_int.floatvaluegroup WHERE "
			"dataproviderid=" << dataproviderId << " AND "
			"placeid=" << placeId << " AND "
			"validtimefrom='" << validfrom << "' AND "
			"validtimeto='" << validto << "' AND "
			"valueparameterid=" << valueparameterId << " AND "
			"levelparameterid=" << levelparameterId << " AND "
			"levelfrom=" << levelfrom << " AND "
			"levelto=" << levelto << " AND "
			"dataversion=" << dataversion;
	//std::cout << query.str() << std::endl;

	pqxx::result r = transaction_->exec(query);

	if ( r.empty() )
	{
		pqxx::result id = transaction_->exec("SELECT nextval('wdb_int.floatvaluegroup_valuegroupid_seq')");
		int valueGroupId = id.at(0).at(0).as<int>();

		std::stringstream query;
		query << "INSERT INTO wdb_int.floatvaluegroup VALUES (" <<
				valueGroupId <<','<<
				dataproviderId <<','<<
				placeId <<','<<
				'\'' << validfrom << "',"<<
				'\'' << validto << "',"<<
				0 <<','<<
				valueparameterId <<','<<
				levelparameterId <<','<<
				levelfrom <<','<<
				levelto <<','<<
				0 <<','<<
				dataversion << ")";

		transaction_->exec(query);

		return valueGroupId;
	}
	return r[0][0].as<int>();
}


} /* namespace fastload */
