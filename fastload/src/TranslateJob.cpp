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

#include "TranslateJob.h"
#include "InputData.h"
#include <wdbLogHandler.h>
#include <pqxx/pqxx>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <iostream>

namespace fastload
{

TranslateJob::TranslateJob(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace, DataQue::Ptr readQue, DataQue::Ptr writeQue, bool failOnSingleError) :
		AbstractJob(writeQue),
		readQue_(readQue),
		failOnSingleError_(failOnSingleError),
		pqConnectString_(pqConnectString),
		wciUser_(wciUser),
		nameSpace_(nameSpace)
{
}

TranslateJob::~TranslateJob()
{
}

void TranslateJob::run()
{
	pqxx::connection connection(pqConnectString_);
	pqxx::work transaction(connection);

	std::string dataprovider;
	std::string input;
	while ( readQue_->get(input) )
	{
		try
		{
			boost::trim(input);

			if ( input.empty() )
				dataprovider = std::string();
			else if ( input[0] == '#' )
				continue;
			else if ( dataprovider.empty() )
				dataprovider = updateDataprovider_(input, transaction);
			else
				que_->put(translate(input, dataprovider, transaction));
		}
		catch ( std::exception & e )
		{
			if ( failOnSingleError_ )
				throw;

			WDB_LOG & log = WDB_LOG::getInstance( "wdb.fastload.job.translate" );
			log.warnStream() << "Error when generating data: " << e.what() << " (from " << input << ")";
			// and continue as if nothing has happened...
		}
	}
	que_->done();
}

std::string TranslateJob::translate(const std::string & what, const std::string & dataprovider, pqxx::work & transaction)
{
	InputData inputData(what, dataprovider);

	char sep = '\t';

	std::ostringstream s;
	s << 1 <<sep<<
			dataproviderid_(dataprovider, transaction) <<sep<<
			placeid_(inputData.placename(), transaction) <<sep<<
			inputData.referencetime() <<sep<<
			inputData.validfrom() <<sep<<
			inputData.validto() <<sep<<
			0 <<sep<<
			valueparameterid_(inputData.valueparametername(), transaction) <<sep<<
			levelparameterid_(inputData.levelparametername(), transaction) <<sep<<
			inputData.levelfrom() <<sep<<
			inputData.levelto() <<sep<<
			0 <<sep<<
			inputData.dataversion() <<sep<<
			inputData.maxdataversion() <<sep<<
			0 <<sep<<
			inputData.value() <<sep<<
			now_(transaction) << '\n';

	return s.str();
}

std::string TranslateJob::updateDataprovider_(const std::string & dataproviderSpec, pqxx::work & transaction)
{
	std::vector<std::string> elements;
	boost::split(elements, dataproviderSpec, boost::is_any_of("\t"));

	if ( elements.size() > 2 or elements.empty() )
		throw std::runtime_error("Invalid spec for dataprovider: " + dataproviderSpec);

	const std::string dataprovider = elements[0];

	if ( elements.size() == 2 or not nameSpace_.empty() )
	{
		std::string ns = nameSpace_.empty() ? elements[1] : nameSpace_;
		transaction.exec("SELECT wci.begin('" + transaction.esc(wciUser_) + "', " + transaction.esc(ns) + ")");
	}
	else // if ( elements.size() == 1 )
		transaction.exec("SELECT wci.begin('" + transaction.esc(wciUser_) + "')");

	return dataprovider;
}

long long TranslateJob::dataproviderid_(const std::string & dataprovidername, pqxx::work & transaction)
{
	long long & dataproviderid = dataproviders_[dataprovidername];
	if ( ! dataproviderid )
	{
		std::ostringstream query;
		query << "SELECT wci_int.idfromdataprovider('" << transaction.esc(dataprovidername) << "')";
		pqxx::result result = transaction.exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized dataprovider: " + dataprovidername);
		dataproviderid = result[0][0].as<long long>();
	}
	return dataproviderid;
}

long long TranslateJob::placeid_(const std::string & placename, pqxx::work & transaction)
{
	long long & placeid = placeids_[placename];
	if ( ! placeid )
	{
		std::ostringstream query;
		query << "SELECT placeid FROM wci.getplacedefinition('" << transaction.esc(placename) << "')";
		pqxx::result result = transaction.exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized placename: " + placename);
		placeid = result[0][0].as<long long>();
	}
	return placeid;
}

int TranslateJob::valueparameterid_(const std::string & parametername, pqxx::work & transaction)
{
	int & paramid = valueparameterids_[parametername];
	if ( ! paramid )
	{
		std::ostringstream query;
		query << "SELECT wci_int.getvalueparameterid(wci_int.normalizeparameter('" << transaction.esc(parametername) << "'))";
		pqxx::result result = transaction.exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized value parameter: " + parametername);
		paramid = result[0][0].as<int>();
	}
	return paramid;
}

int TranslateJob::levelparameterid_(const std::string & parametername, pqxx::work & transaction)
{
	int & paramid = levelparameterids_[parametername];
	if ( ! paramid )
	{
		std::ostringstream query;
		query << "SELECT wci_int.getlevelparameterid(wci_int.normalizelevelparameter('" << transaction.esc(parametername) << "'))";
		pqxx::result result = transaction.exec(query.str());
		if ( result.empty() )
			throw std::runtime_error("Not a recognized level parameter: " + parametername);
		paramid = result[0][0].as<int>();
	}
	return paramid;
}

std::string TranslateJob::now_(pqxx::work & transaction)
{
	if ( timeNow_.empty() )
	{
		pqxx::result result = transaction.exec("SELECT now()");
		timeNow_ = result[0][0].as<std::string>();
	}
	return timeNow_;
}

}
