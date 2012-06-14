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
#include "DatabaseTranslator.h"
#include "InputData.h"
#include <boost/algorithm/string.hpp>
#include <vector>
#include <iostream>

namespace fastload
{

TranslateJob::TranslateJob(const std::string & pqConnectString, const std::string & wciUser, const std::string & nameSpace, DataQueue::Ptr readQueue, DataQueue::Ptr writeQueue) :
		AbstractJob(writeQueue),
		readQueue_(readQueue),
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
	try
	{
		DatabaseTranslator translator(pqConnectString_, wciUser_, nameSpace_);

		if ( tableStructure_(translator) == New )
			queue_->put("COPY wdb_int.floatvalueitem (valuegroupid, referencetime, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN");
		else
			queue_->put("COPY wdb_int.floatvalue (valuetype, dataproviderid, placeid, referencetime, validtimefrom, validtimeto, validtimeindeterminatecode, valueparameterid, levelparameterid, levelfrom, levelto, levelindeterminatecode, dataversion, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN");


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
				dataprovider = translator.updateDataprovider(input);
			else
			{
				std::string nextLine = getCopyStatement_(input, dataprovider, translator);
				queue_->put(nextLine);
			}
		}
	}
	catch ( std::exception & )
	{
		readQueue_->shutdown();
		throw;
	}
	queue_->done();
}

namespace
{
int getValueGroup()
{
	return 0;
}
}

std::string TranslateJob::getCopyStatement_(const std::string & what, const std::string & dataprovider, DatabaseTranslator & translator)
{
	InputData inputData(what, dataprovider);

	if ( tableStructure_(translator) == New )
	{
		int valueGroup = translator.getValueGroup(
				inputData.dataprovider(),
				inputData.placename(),
				inputData.validfrom() - inputData.referencetime(),
				inputData.validto() - inputData.referencetime(),
				inputData.valueparametername(),
				inputData.levelparametername(), inputData.levelfrom(), inputData.levelto(),
				inputData.dataversion());

		char sep = '\t';
		std::ostringstream s;
		s << valueGroup <<sep<<
				inputData.referencetime() <<sep<<
				inputData.maxdataversion() <<sep<<
				0 <<sep<<
				inputData.value() <<sep<<
				translator.now() << '\n';

		return s.str();
	}
	else
	{
		char sep = '\t';
		std::ostringstream s;
		s << 1 <<sep<<
				translator.dataproviderid(dataprovider) <<sep<<
				translator.placeid(inputData.placename()) <<sep<<
				inputData.referencetime() <<sep<<
				inputData.validfrom() <<sep<<
				inputData.validto() <<sep<<
				0 <<sep<<
				translator.valueparameterid(inputData.valueparametername()) <<sep<<
				translator.levelparameterid(inputData.levelparametername()) <<sep<<
				inputData.levelfrom() <<sep<<
				inputData.levelto() <<sep<<
				0 <<sep<<
				inputData.dataversion() <<sep<<
				inputData.maxdataversion() <<sep<<
				0 <<sep<<
				inputData.value() <<sep<<
				translator.now() << '\n';
		return s.str();
	}
}

TranslateJob::WdbInternalTableStructure TranslateJob::tableStructure_(DatabaseTranslator & translator)
{
	if ( translator.wciVersion() >= "WDB 1.3.0")
		return New;
	return Old;
}

} /* namespace fastload */
