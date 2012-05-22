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

#include "CopyJob.h"

namespace fastload
{

CopyJob::CopyJob(const std::string & pqConnectString, DataQue::Ptr que) :
		AbstractDatabaseJob(pqConnectString, que)
{
}

CopyJob::~CopyJob()
{
}

void CopyJob::beginCopy(PGconn * connection)
{
	boost::shared_ptr<PGresult> result(
			PQexec(connection, "COPY wdb_int.floatvalue (valuetype, dataproviderid, placeid, referencetime, validtimefrom, validtimeto, validtimeindeterminatecode, valueparameterid, levelparameterid, levelfrom, levelto, levelindeterminatecode, dataversion, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN"),
			PQclear
	);
	checkResult(PGRES_COPY_IN, result.get());
}

void CopyJob::copyRow(PGconn * connection, const std::string & row)
{
	int ok = PQputCopyData(connection, row.c_str(), row.size());
	if ( ok == -1 )
		throw std::runtime_error("wtf??");
	checkResult(ok, 1, "error on copy: (" + row + ")");
}

void CopyJob::endCopy(PGconn * connection)
{
	int ok = PQputCopyEnd(connection, 0);
	if ( ok == -1 )
		throw std::runtime_error("wtf x2??");
	checkResult(ok, 1, "error on end copy");

	boost::shared_ptr<PGresult> finalResult(PQgetResult(connection), PQclear);
	checkResult(PGRES_COMMAND_OK, finalResult.get());
}

void CopyJob::performQueries(PGconn * connection)
{
	beginCopy(connection);

	std::string data;
	while ( que_->get(data) )
		copyRow(connection, data);

	endCopy(connection);
}

}
