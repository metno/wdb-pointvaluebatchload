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

#ifndef TRANSLATEJOB_H_
#define TRANSLATEJOB_H_

#include "AbstractJob.h"
#include <pqxx/transaction>
#include <boost/shared_ptr.hpp>
#include <map>

namespace fastload
{
class DatabaseTranslator;


/**
 * Translates fastload's input format into COPY statements for feeding into a
 * wdb database.
 */
class TranslateJob : public AbstractJob
{
public:
	virtual ~TranslateJob();

	typedef boost::shared_ptr<TranslateJob> Ptr;

	static Ptr get(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace, DataQueue::Ptr readQueue, DataQueue::Ptr writeQueue, bool forwardWrites = true);

protected:
	virtual void run();

	TranslateJob(DataQueue::Ptr readQueue, DataQueue::Ptr writeQueue, boost::shared_ptr<DatabaseTranslator> translator, bool forwardWrites);
	TranslateJob(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace, DataQueue::Ptr readQueue, DataQueue::Ptr writeQueue, bool forwardWrites = true);

	virtual std::string getCopyCommand() =0;
	virtual std::string getCopyStatement(const std::string & what, const std::string & dataprovider) =0;


	DataQueue::Ptr readQueue_;
	boost::shared_ptr<DatabaseTranslator> translator_;

private:
//	std::string getNewCopyStatement_(const std::string & what, const std::string & dataprovider);
//	std::string getOldCopyStatement_(const std::string & what, const std::string & dataprovider);
//
//	enum WdbInternalTableStructure
//	{
//		Old, New
//	};
//	WdbInternalTableStructure tableStructure_();
};

} /* namespace fastload */
#endif /* TRANSLATEJOB_H_ */
