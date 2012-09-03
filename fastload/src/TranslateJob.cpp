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
	if ( forwardWrites )
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
		queue_->put(getCopyCommand());

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
				std::string nextLine = getCopyStatement(input, dataprovider);
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

//std::string TranslateJob::getCopyCommand()
//{
//	if ( tableStructure_() == New )
//		return "COPY wdb_int.floatvalueitem (valuegroupid, referencetime, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN";
//
//	return "COPY wdb_int.floatvalue (valuetype, dataproviderid, placeid, referencetime, validtimefrom, validtimeto, validtimeindeterminatecode, valueparameterid, levelparameterid, levelfrom, levelto, levelindeterminatecode, dataversion, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN";
//}
//
//std::string TranslateJob::getCopyStatement(const std::string & what, const std::string & dataprovider)
//{
//	if ( tableStructure_() == New )
//		getNewCopyStatement_(what, dataprovider);
//	else
//		getOldCopyStatement_(what, dataprovider);
//}
//
//std::string TranslateJob::getNewCopyStatement_(const std::string & what, const std::string & dataprovider)
//{
//	InputData inputData(what, dataprovider);
//
//	int valueGroup = translator_->getValueGroup(
//			inputData.dataprovider(),
//			inputData.placename(),
//			inputData.referencetime(),
//			inputData.validfrom() - inputData.referencetime(),
//			inputData.validto() - inputData.referencetime(),
//			inputData.valueparametername(),
//			inputData.levelparametername(), inputData.levelfrom(), inputData.levelto(),
//			inputData.dataversion());
//
//	char sep = '\t';
//	std::ostringstream s;
//	s << valueGroup <<sep<<
//			inputData.referencetime() <<sep<<
//			inputData.maxdataversion() <<sep<<
//			0 <<sep<<
//			inputData.value() <<sep<<
//			translator_->now() << '\n';
//
//	return s.str();
//}
//
//std::string TranslateJob::getOldCopyStatement_(const std::string & what, const std::string & dataprovider)
//{
//	InputData inputData(what, dataprovider);
//
//	char sep = '\t';
//	std::ostringstream s;
//	s << 1 <<sep<<
//			translator_->dataproviderid(dataprovider) <<sep<<
//			translator_->placeid(inputData.placename(), inputData.validto()) <<sep<<
//			inputData.referencetime() <<sep<<
//			inputData.validfrom() <<sep<<
//			inputData.validto() <<sep<<
//			0 <<sep<<
//			translator_->valueparameterid(inputData.valueparametername()) <<sep<<
//			translator_->levelparameterid(inputData.levelparametername()) <<sep<<
//			inputData.levelfrom() <<sep<<
//			inputData.levelto() <<sep<<
//			0 <<sep<<
//			inputData.dataversion() <<sep<<
//			inputData.maxdataversion() <<sep<<
//			0 <<sep<<
//			inputData.value() <<sep<<
//			translator_->now() << '\n';
//	return s.str();
//}
//
//
//TranslateJob::WdbInternalTableStructure TranslateJob::tableStructure_()
//{
//	if ( translator_->wciVersion() >= "WDB 1.3.0")
//		return New;
//	return Old;
//}

} /* namespace fastload */
