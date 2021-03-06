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
namespace old
{

CopyJob::CopyJob(const std::string & pqConnectString, DataQueue::Ptr queue) :
		AbstractDatabaseJob(pqConnectString, queue)
{
}

CopyJob::~CopyJob()
{
	endCopy();
}

//void CopyJob::beginCopy(PGconn * connection)
//{
//	//const char * copyStatement = "COPY wdb_int.floatvalue (valuetype, dataproviderid, placeid, referencetime, validtimefrom, validtimeto, validtimeindeterminatecode, valueparameterid, levelparameterid, levelfrom, levelto, levelindeterminatecode, dataversion, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN";
//	const char * copyStatement = "COPY wdb_int.floatvalueitem (valuegroupid, referencetime, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN";
//
//	boost::shared_ptr<PGresult> result(
//			PQexec(connection, copyStatement),
//			PQclear
//	);
//	checkResult(PGRES_COPY_IN, result.get());
//}

void CopyJob::copyRow(const std::string & row)
{
	//std::cout << row << std::endl;

	if ( row[0] == 'C' )
	{
		if ( row.substr(0, 5) == "COPY " )
		{
			boost::shared_ptr<PGresult> result(
					PQexec(copyConnection_.get(), row.c_str()),
					PQclear
			);
			checkResult(PGRES_COPY_IN, result.get());
			return;
		}
	}

	int ok = PQputCopyData(copyConnection_.get(), row.c_str(), row.size());
	if ( ok == -1 )
		throw std::runtime_error("wtf??");
	checkResult(ok, 1, "error on copy: (" + row + ")");
}

void CopyJob::endCopy()
{
	if ( copyConnection_ )
	{
		int ok = PQputCopyEnd(copyConnection_.get(), 0);
		if ( ok == -1 )
			throw std::runtime_error("wtf x2??");
		checkResult(ok, 1, "error on end copy");

		boost::shared_ptr<PGresult> finalResult(PQgetResult(copyConnection_.get()), PQclear);
		checkResult(PGRES_COMMAND_OK, finalResult.get());
	}
}

void CopyJob::performQueries(CopyJob::Connection connection)
{
	endCopy();
	std::string data;
	if ( queue_->get(data) )
	{
		copyConnection_ = connection;
		copyRow(data);
		while ( queue_->get(data) )
			copyRow(data);
	}
}

}
}
