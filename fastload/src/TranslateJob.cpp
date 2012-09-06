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
#include "OldStyleTranslateJob.h"
#include "NewStyleTranslateJob.h"
#include "DatabaseTranslator.h"
#include "InputData.h"
#include <boost/algorithm/string.hpp>
#include <vector>
#include <iostream>

namespace fastload
{

TranslateJob::TranslateJob(const std::string & pqConnectString,
							const std::string & wciUser,
							const std::string & nameSpace,
							DataQueue::Ptr readQueue,
							DataQueue::Ptr writeQueue,
							bool forwardWrites) :
		AbstractJob(writeQueue),
		readQueue_(readQueue),
		translator_(new DatabaseTranslator(pqConnectString, wciUser, nameSpace))
{
	if ( not forwardWrites )
		queue_->done();
}

TranslateJob::TranslateJob(	DataQueue::Ptr readQueue, DataQueue::Ptr writeQueue, boost::shared_ptr<DatabaseTranslator> translator, bool forwardWrites ) :
		AbstractJob(writeQueue),
		readQueue_(readQueue),
		translator_(translator)
{
	if ( not forwardWrites )
		queue_->done();
}

TranslateJob::~TranslateJob()
{
}

TranslateJob::Ptr TranslateJob::get(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace, DataQueue::Ptr readQueue, DataQueue::Ptr writeQueue, bool forwardWrites)
{
	boost::shared_ptr<DatabaseTranslator> translator(new DatabaseTranslator(pqConnectString, wciUser, nameSpace));

	if ( translator->wciVersion() >= "WDB 1.3.0" )
		return Ptr(new NewStyleTranslateJob(readQueue, writeQueue, translator, forwardWrites));
	else
		return Ptr(new OldStyleTranslateJob(readQueue, writeQueue, translator, forwardWrites));
}

void TranslateJob::run()
{
	try
	{
		std::string copyQuery;
		std::string dataprovider;
		std::string input;
		while ( readQueue_->get(input) )
		{
			boost::trim(input);

			if ( input.empty() )
				dataprovider = std::string();
			else if ( input[0] == '#' )
				continue;
			else if ( dataprovider.empty() )
				dataprovider = translator_->updateDataprovider(input);
			else
			{
				InputData inputData(input, dataprovider);
				std::string newCopyQuery = getCopyQuery(inputData);
				if ( newCopyQuery != copyQuery)
				{
					copyQuery = newCopyQuery;
					queue_->put(copyQuery);
				}
				std::string nextLine = getCopyStatement(inputData);
				queue_->put(nextLine);
			}
		}
		translator_->commit();
	}
	catch ( std::exception & )
	{
		readQueue_->shutdown();
		throw;
	}
	queue_->done();
}

} /* namespace fastload */
