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

#include "OldStyleTranslateJob.h"
#include "DatabaseTranslator.h"
#include "InputData.h"


namespace fastload
{

OldStyleTranslateJob::OldStyleTranslateJob(DataQueue::Ptr readQueue, DataQueue::Ptr writeQueue, boost::shared_ptr<DatabaseTranslator> translator, bool forwardWrites) :
		TranslateJob(readQueue, writeQueue, translator, forwardWrites)
{
}

OldStyleTranslateJob::~OldStyleTranslateJob()
{
}

std::string OldStyleTranslateJob::getCopyQuery(const InputData &)
{
	return "COPY wdb_int.floatvalue (valuetype, dataproviderid, placeid, referencetime, validtimefrom, validtimeto, validtimeindeterminatecode, valueparameterid, levelparameterid, levelfrom, levelto, levelindeterminatecode, dataversion, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN";
}

std::string OldStyleTranslateJob::getCopyStatement(const InputData & inputData)
{
	char sep = '\t';
	std::ostringstream s;
	s << 1 <<sep<<
			translator_->dataproviderid(inputData.dataprovider()) <<sep<<
			translator_->placeid(inputData.placename(), inputData.validto()) <<sep<<
			inputData.referencetime() <<sep<<
			inputData.validfrom() <<sep<<
			inputData.validto() <<sep<<
			0 <<sep<<
			translator_->valueparameterid(inputData.valueparametername()) <<sep<<
			translator_->levelparameterid(inputData.levelparametername()) <<sep<<
			inputData.levelfrom() <<sep<<
			inputData.levelto() <<sep<<
			0 <<sep<<
			inputData.dataversion() <<sep<<
			inputData.maxdataversion() <<sep<<
			0 <<sep<<
			inputData.value() <<sep<<
			translator_->now() << '\n';
	return s.str();
}


}
