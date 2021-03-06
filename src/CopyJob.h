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

#ifndef COPYTHREAD_H_
#define COPYTHREAD_H_

#include "AbstractDatabaseJob.h"

namespace fastload
{
namespace old
{

/**
 * Sends sql COPY statements, read from the given queue, to a database. The
 * sql COPY statement to be used is expected to be delivered first to the
 * queue.
 */
class CopyJob : public AbstractDatabaseJob
{
public:
	CopyJob(const std::string & pqConnectString, DataQueue::Ptr queue);
	~CopyJob();

protected:
	virtual void performQueries(Connection connection);

private:
	//void beginCopy(PGconn * connection);
	void copyRow(const std::string & row);
	void endCopy();

	Connection copyConnection_;
};

}
}
#endif /* COPYTHREAD_H_ */
