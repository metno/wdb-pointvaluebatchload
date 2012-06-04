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

#include "AbstractDatabaseJob.h"
#include <stdexcept>

namespace fastload
{
namespace
{
typedef boost::shared_ptr<PGconn> Connection;
Connection connect(const char * conninfo)
{
	return Connection(PQconnectdb(conninfo), PQfinish);
}
}

AbstractDatabaseJob::AbstractDatabaseJob(const std::string & pqConnectString, DataQueue::Ptr que) :
		AbstractJob(que),
		pqConnectString_(pqConnectString)
{
}

AbstractDatabaseJob::~AbstractDatabaseJob()
{
}

void AbstractDatabaseJob::run()
{
	Connection connection = connect(pqConnectString_.c_str());
	if ( ! connection )
		throw std::runtime_error("Unable to connect to database");

	performQueries(connection.get());
}

void AbstractDatabaseJob::checkResult(int what, int expected, const std::string & message) const
{
	if ( what != expected )
		throw std::runtime_error(message);
}

void AbstractDatabaseJob::checkResult(ExecStatusType est, PGresult * result) const
{
	if ( PQresultStatus(result) != est )
		throw std::runtime_error(PQresultErrorMessage(result));
}


} /* namespace fastload */
