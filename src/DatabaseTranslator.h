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
#include "timetypes.h"
#include <pqxx/connection.hxx>
#include <pqxx/transaction>
#include <pqxx/tablewriter.hxx>
#include <boost/noncopyable.hpp>
#include <map>
#include <set>


namespace fastload
{

/**
 * Translates text from fastload's input format into values matching wdb's
 * internal structures.
 *
 * All values referred here are cached in memory, so you can call each
 * function as many times as you like without worrying about response time.
 */
class DatabaseTranslator : boost::noncopyable
{
public:
	DatabaseTranslator(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace);
	~DatabaseTranslator();

	/**
	 * Modify dataprovider name and namespace to use as basis for other queries.
	 *
	 * @param dataproviderSpec dataprovider and (optionally) namespace
	 *          specification, of the same format as read by fastload.
	 * @returns The parsed dataprovider name
	 */
	std::string updateDataprovider(const std::string & dataproviderSpec);

	/**
	 * Get wdb's internal value for the given data provider name
	 *
	 * @throws std::runtime_error if unable to find any id for the given name
	 */
	long long dataproviderid(const std::string & dataprovidername);

	/**
	 * Get wdb's internal value for the given data place name, valid at the
	 * given time.
	 *
	 * @throws std::runtime_error if unable to find any id for the given name
	 */
	long long placeid(const std::string & placename, const Time & time);

	/**
	 * Get wdb's internal value for the given value parameter name
	 *
	 * @throws std::runtime_error if unable to find any id for the given name
	 */
	int valueparameterid(const std::string & parametername);

	/**
	 * Get wdb's internal value for the given level name
	 *
	 * @throws std::runtime_error if unable to find any id for the given name
	 */
	int levelparameterid(const std::string & parametername);

	/**
	 * Get the database's time for this transaction. This value will not
	 * change on on later calls.
	 *
	 * @returns A string representation of the time now, in the database's locale.
	 */
	std::string now();

	/**
	 * Get the value group id for the given parameters. This may cause new
	 * rows to be created in the database. Therefore, you should call commit()
	 * if you use this method.
	 *
	 * If called on wciVersion() < 1.3.0, it should cause some kind of
	 * exception.
	 */
	int getValueGroup(const std::string & dataprovidername,
			const std::string & placename,
			const Time & referenceTime,
			const Duration & validTimeFrom,
			const Duration & validTimeTo,
			const std::string & valueparametername,
			const std::string & levelparametername,
			float levelfrom,
			float levelto,
			int dataversion);

	/**
	 * If the database table is partitioned, get the name of the destination
	 * table to store data into, bypassing any data reallocation triggers in
	 * the database.
	 *
	 * If the database is not partitioned, return an empty string.
	 *
	 * This way of storing data is useful, since mixing triggers and COPY
	 * statements seems to create some problems with entering new
	 * floatvalue groups into the database.
	 *
	 * This will not work for paritioned old-style databases.
	 */
	std::string getDataTableName(const Time & referenceTime);

	/**
	 * Get the version of the wdb database we are connected to. This gives the
	 * same result as saying "SELECT wci.version()" in psql.
	 */
	std::string wciVersion();

	/**
	 * Commit all changes done on this database, if any. This may be required
	 * if getValueGroup(...) has been called.
	 */
	void commit();

	struct TablePartition
	{
		TablePartition(const std::string & tableName, const Time & validFrom, const Time & validTo) :
			tableName(tableName), validFrom(validFrom), validTo(validTo)
		{}
		std::string tableName;
		Time validFrom;
		Time validTo;
	};

private:
	pqxx::work & transaction();

	pqxx::result exec(const std::string & query);

	//TODO. Refactoring!

	/**
	 * Database connection for querying wdb for whatever values we need.
	 */
	pqxx::connection connection_;

	/**
	 * Transaction associated with connection_ object
	 */
	pqxx::work * transaction_;

	/**
	 * wci user for wci.begin call
	 */
	std::string wciUser_;

	/**
	 * namespace sepcification for wci.begin call
	 */
	std::string nameSpace_;

	/**
	 * Database connection reserved for copying new floatvaluegroup entries
	 */
	pqxx::lazyconnection copyConnection_;
	/**
	 * Transaction associated with copyConnection_
	 */
	pqxx::work * copyTransaction_;
	/**
	 * writer for floatvaluegroup entries. Associated with copyTransaction_
	 */
	pqxx::tablewriter * tableWriter_;

	std::map<std::string, long long> dataproviders_;
	std::map<std::string, std::map<Time, long long> > placeids_;
	std::map<std::string, int> valueparameterids_;
	std::map<std::string, int> levelparameterids_;
	std::string timeNow_;
	std::string wciVersion_;

	/// List of floatvaluegroup entries, with their id numbers
	std::map<FloatValueGroup, int> floatValueGroups_;

	/**
	 * List over queried data providers and place names. We use this to avoid
	 * calling the database many times with the same query.
	 */
	std::set<std::pair<long long, long long> > queriedDataprovidersAndPlaces_;


	std::vector<TablePartition> destinationTables_;
};

} /* namespace fastload */
#endif /* DATABASETRANSLATOR_H_ */
